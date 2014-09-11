require 'socket'
require 'thread'

module ParallelServer
  class Prefork
    DEFAULT_MIN_PROCESSES = 5
    DEFAULT_MAX_PROCESSES = 20
    DEFAULT_MAX_THREADS = 1
    DEFAULT_STANDBY_THREADS = 5
    DEFAULT_MAX_IDLE = 10

    attr_reader :child_status

    # @!macro [new] args
    #   @param host [String] hostname or IP address
    #   @param port [Integer / String] port number / service name
    #   @param opts [Hash] options
    #   @option opts [Integer] :min_processes (5) minimum processes
    #   @option opts [Integer] :max_processes (20) maximum processes
    #   @option opts [Integer] :max_idle (10) cihld process exits if max_idle seconds is expired
    #   @option opts [Integer] :max_threads (1) maximum threads per process
    #   @option opts [Integer] :standby_threads (5) keep free processes or threads
    #   @option opts [Integer] :listen_backlog (nil) listen backlog
    #   @option opts [#call] :on_start (nil) object#call() is invoked when child process start. This is called in child process.
    #   @option opts [#call] :on_child_start (nil) object#call(pid) is invoked when child process exit. This is call in parent process.
    #   @option opts [#call] :on_child_exit (nil) object#call(pid, status) is invoked when child process exit. This is call in parent process.

    # @overload initialize(host=nil, port, opts={})
    #   @!macro args
    def initialize(*args)
      host, port, opts = parse_args(*args)
      @host, @port, @opts = host, port, opts
      set_variables_from_opts
      @from_child = {}             # IO => pid
      @to_child = {}               # pid => IO
      @child_status = {}           # pid => Hash
      @children = []               # pid
      @thread_to_child = {}        # pid => Thread
      @loop = true
    end

    # @return [void]
    # @yield [sock, addr, child]
    # @yieldparam sock [Socket]
    # @yieldparam addr [Addrinfo]
    # @yieldparam child [ParallelServer::Prefork::Child]
    def start(&block)
      raise 'block required' unless block
      @block = block
      @sockets = Socket.tcp_server_sockets(@host, @port)
      @sockets.each{|s| s.listen(@listen_backlog)} if @listen_backlog
      @reload_args = nil
      while @loop
        do_reload if @reload_args
        watch_children
        adjust_children
      end
    ensure
      @sockets.each{|s| s.close rescue nil}
      @to_child.values.each{|s| s.close rescue nil}
      @to_child.clear
      @thread_to_child.values.each(&:exit)
      @thread_to_child.clear
      Timeout.timeout(1){wait_all_children} rescue Thread.new{wait_all_children}
    end

    # @overload reload(host=nil, port, opts={})
    #   @macro args
    # @return [void]
    def reload(*args)
      @reload_args = parse_args(*args)
    end

    # @return [void]
    def do_reload
      host, port, @opts = @reload_args
      @reload_args = nil
      old_listen_backlog = @listen_backlog
      set_variables_from_opts

      address_changed = false
      if @host != host || @port != port
        @host, @port = host, port
        @sockets.each(&:close)
        @sockets = Socket.tcp_server_sockets(@host, @port)
        @sockets.each{|s| s.listen(@listen_backlog)} if @listen_backlog
        address_changed = true
      elsif @listen_backlog != old_listen_backlog
        @sockets.each{|s| s.listen(@listen_backlog)} if @listen_backlog
      end

      reload_children(address_changed)
    end

    # @param address_changed [true/false]
    # @return [void]
    def reload_children(address_changed=false)
      data = {}
      data[:address_changed] = address_changed
      data[:options] = @opts.select{|_, value| Marshal.dump(value) rescue nil}
      talk_to_children data
    end

    # @param data [String]
    # @return [void]
    def talk_to_children(data)
      @data_to_child = Marshal.dump(data)
      @thread_to_child.values.each do |thr|
        begin
          thr.run
        rescue ThreadError
          # try to run dead thread. ignore it.
        end
      end
    end

    # @param io [IO]
    # @return [void]
    def talk_to_child_loop(io)
      data = nil
      while true
        Thread.stop if data.nil? || data == @data_to_child
        data = @data_to_child
        Conversation._send(io, data)
      end
    end

    # @return [void]
    def stop
      @loop = false
    end

    # @return [void]
    def stop!
      Process.kill 'TERM', *@children rescue nil
      @loop = false
    end

    # @overload parse_args(host=nil, port, opts={})
    #   @macro args
    # @return [Array<String, String, Hash>] hostname, port, option
    def parse_args(*args)
      opts = {}
      arg_count = args.size
      if args.last.is_a? Hash
        opts = args.pop
      end
      if args.size == 1
        host, port = nil, args.first
      elsif args.size == 2
        host, port = args
      else
        raise ArgumentError, "wrong number of arguments (#{arg_count} for 1..3)"
      end
      return host, port, opts
    end

    # @return [Integer]
    def watch_children
      rset = @from_child.empty? ? nil : @from_child.keys
      readable, = IO.select(rset, nil, nil, 0.1)
      if readable
        readable.each do |from_child|
          pid = @from_child[from_child]
          if st = Conversation.recv(from_child)
            @child_status[pid].update st
            if st[:status] == :stop
              @to_child[pid].close rescue nil
              @to_child.delete pid
              @thread_to_child[pid].exit rescue nil
              @thread_to_child.delete pid
            end
          else
            @from_child.delete from_child
            @to_child[pid].close rescue nil
            @to_child.delete pid
            @thread_to_child[pid].exit rescue nil
            @thread_to_child.delete pid
            @child_status.delete pid
            from_child.close
          end
        end
      end
      if @children.size != @child_status.size
        wait_children
      end
    end

    # @return [void]
    def adjust_children
      (@min_processes - available_children).times do
        start_child
      end
      capa, conn = current_capacity_and_connections
      required_connections = conn + @standby_threads
      required_processes = (required_connections - capa + @max_threads - 1) / @max_threads
      [required_processes, @max_processes - available_children].min.times do
        start_child
      end
    end

    # current capacity and current connections
    # @return [Array<Integer, Integer>]
    def current_capacity_and_connections
      values = @child_status.values
      capa = values.count{|st| st[:status] == :run} * @max_threads
      conn = values.map{|st| st[:connections].count}.reduce(&:+).to_i
      return [capa, conn]
    end

    # @return [Integer]
    def available_children
      @child_status.values.count{|st| st[:status] == :run}
    end

    # @return [void]
    def wait_children
      @children.delete_if do |pid|
        _pid, status = Process.waitpid2(pid, Process::WNOHANG)
        @on_child_exit.call(pid, status) if _pid && @on_child_exit
        _pid
      end
    end

    # @return [void]
    def wait_all_children
      until @children.empty?
        watch_children
      end
    end

    # @return [void]
    def start_child
      from_child = IO.pipe
      to_child = IO.pipe
      pid = fork do
        @from_child.keys.each(&:close)
        @to_child.values.each(&:close)
        from_child[0].close
        to_child[1].close
        @on_start.call if @on_start
        Child.new(@sockets, @opts, from_child[1], to_child[0]).start(@block)
      end
      from_child[1].close
      to_child[0].close
      @from_child[from_child[0]] = pid
      @to_child[pid] = to_child[1]
      @thread_to_child[pid] = Thread.new(to_child[1]){|io| talk_to_child_loop(io)}
      @children.push pid
      @child_status[pid] = {status: :run, connections: {}}
      @on_child_start.call(pid) if @on_child_start
    end

    private

    def set_variables_from_opts
      @min_processes = @opts[:min_processes] || DEFAULT_MIN_PROCESSES
      @max_processes = @opts[:max_processes] || DEFAULT_MAX_PROCESSES
      @max_threads = @opts[:max_threads] || DEFAULT_MAX_THREADS
      @standby_threads = @opts[:standby_threads] || DEFAULT_STANDBY_THREADS
      @listen_backlog = @opts[:listen_backlog]
      @on_start = @opts[:on_start]
      @on_child_start = @opts[:on_child_start]
      @on_child_exit = @opts[:on_child_exit]
    end

    class Child

      attr_reader :options

      # @param sockets [Array<Socket>]
      # @param opts [Hash]
      # @param to_parent [IO]
      # @param from_parent [IO]
      def initialize(sockets, opts, to_parent, from_parent)
        @sockets = sockets
        @options = opts
        @to_parent = to_parent
        @from_parent = from_parent
        @threads = {}
        @threads_mutex = Mutex.new
        @threads_cv = ConditionVariable.new
        @parent_mutex = Mutex.new
        @status = :run
      end

      # @return [Integer]
      def max_threads
        @options[:max_threads] || DEFAULT_MAX_THREADS
      end

      # @return [Integer]
      def max_idle
        @options[:max_idle] || DEFAULT_MAX_IDLE
      end

      # @param block [#call]
      # @return [void]
      def start(block)
        first = true
        while @status == :run
          wait_thread
          sock, addr = accept(first)
          break unless sock
          first = false
          thr = Thread.new(sock, addr){|s, a| run(s, a, block)}
          @threads_mutex.synchronize do
            @threads[thr] = addr
          end
        end
        @status = :stop
        @sockets.each(&:close)
        @threads_mutex.synchronize do
          notify_status
        end
        wait_all_connections
      end

      # @return [void]
      def wait_all_connections
        @threads.keys.each do |thr|
          thr.join rescue nil
        end
      end

      # @return [void]
      def reload
        data = Conversation.recv(@from_parent)
        raise if data[:address_changed]
        @options.update data[:options]
      rescue
        @status = :stop
      end

      # @return [void]
      def wait_thread
        @threads_mutex.synchronize do
          while @threads.size >= max_threads
            @threads_cv.wait(@threads_mutex)
          end
        end
      end

      # @param addr [Addrinfo]
      # @return [void]
      def connected(addr)
        @threads_mutex.synchronize do
          @threads[Thread.current] = addr
          notify_status
        end
      end

      # @return [void]
      def disconnect
        @threads_mutex.synchronize do
          @threads.delete Thread.current
          notify_status
          @threads_cv.signal
        end
      end

      # @return [void]
      def notify_status
        connections = Hash[@threads.map{|thr, adr| [thr.object_id, adr]}]
        status = {
          status: @status,
          connections: connections,
        }
        Conversation.send(@to_parent, status)
      rescue Errno::EPIPE
        # ignore
      end

      # @param sock [Socket]
      # @param addr [AddrInfo]
      # @param block [#call]
      # @return [void]
      def run(sock, addr, block)
        connected(addr)
        block.call(sock, addr, self)
      rescue Exception => e
        STDERR.puts e.inspect, e.backtrace.inspect
      ensure
        sock.close rescue nil
        disconnect
      end

      # @param first [Boolean]
      # @return [Array<Socket, AddrInfo>]
      # @return [nil]
      def accept(first=nil)
        while true
          timer = first ? nil : max_idle
          readable, = IO.select(@sockets+[@from_parent], nil, nil, timer)
          return nil unless readable
          r, = readable
          if r == @from_parent
            reload
            next if @status == :run
            return nil
          end
          begin
            sock, addr = r.accept_nonblock
            return [sock, addr]
          rescue IO::WaitReadable
            next
          end
        end
      end
    end

    class Conversation
      # @param io [IO]
      # @param msg [Object]
      # @return [void]
      def self.send(io, msg)
        _send(io, Marshal.dump(msg))
      end

      # @param io [IO]
      # @param data [String] marshaled data
      # @return [void]
      def self._send(io, data)
        io.puts data.length
        io.write data
      end

      # @param io [IO]
      # @return [Object]
      def self.recv(io)
        len = io.gets
        return unless len && len =~ /\A\d+\n/
        len = len.to_i
        data = io.read(len)
        return unless data && data.size == len
        Marshal.load(data)
      end
    end
  end
end
