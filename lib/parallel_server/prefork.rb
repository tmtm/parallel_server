require 'socket'
require 'thread'

module ParallelServer
  class Prefork
    DEFAULT_MIN_PROCESSES = 5
    DEFAULT_MAX_PROCESSES = 20
    DEFAULT_MAX_THREADS = 1
    DEFAULT_STANDBY_THREADS = 5
    DEFAULT_MAX_IDLE = 10
    DEFAULT_MAX_USE = 1000
    DEFAULT_WATCHDOG_TIMER = 600
    DEFAULT_WATCHDOG_SIGNAL = 'TERM'

    attr_reader :child_status

    # @!macro [new] args
    #   @param opts [Hash] options
    #   @option opts [Integer] :min_processes (5) minimum processes
    #   @option opts [Integer] :max_processes (20) maximum processes
    #   @option opts [Integer] :max_idle (10) cihld process exits if max_idle seconds is expired
    #   @option opts [Integer] :max_use (1000) child process exits if it is connected max_use times.
    #   @option opts [Integer] :max_threads (1) maximum threads per process
    #   @option opts [Integer] :standby_threads (5) keep free processes or threads
    #   @option opts [Integer] :listen_backlog (nil) listen backlog
    #   @option opts [Integer] :watchdog_timer (600) watchdog timer
    #   @option opts [Integer] :watchdog_signal ('TERM') this signal is sent when watchdog timer expired.
    #   @option opts [#call] :on_start (nil) object#call() is invoked when child process start. This is called in child process.
    #   @option opts [#call] :on_reload (nil) object#call(hash) is invoked when reload. This is called in child process.
    #   @option opts [#call] :on_child_start (nil) object#call(pid) is invoked when child process exit. This is call in parent process.
    #   @option opts [#call] :on_child_exit (nil) object#call(pid, status) is invoked when child process exit. This is call in parent process.

    # @overload initialize(host=nil, port, opts={})
    #   @param host [String] hostname or IP address
    #   @param port [Integer / String] port number / service name
    #   @macro args
    # @overload initialize(socket, opts={})
    #   @param socket [Socket] listening socket
    #   @macro args
    # @overload initialize(sockets, opts={})
    #   @param sockets [Array<Socket>] listening sockets
    #   @macro args
    def initialize(*args)
      @sockets, @host, @port, @opts = parse_args(*args)
      set_variables_from_opts
      @from_child = {}             # IO(r) => pid
      @to_child = {}               # IO(r) => IO(w)
      @child_status = {}           # IO(r) => Hash
      @children = []               # pid
      @loop = true
      @sockets_created = false
    end

    # @return [void]
    # @yield [sock, addr, child]
    # @yieldparam sock [Socket]
    # @yieldparam addr [Addrinfo]
    # @yieldparam child [ParallelServer::Prefork::Child]
    def start(&block)
      raise 'block required' unless block
      @block = block
      unless @sockets
        @sockets = create_server_socket(@host, @port, @listen_backlog)
        @sockets_created = true
      end
      @reload_args = nil
      while @loop
        do_reload if @reload_args
        watch_children
        adjust_children
      end
    ensure
      @sockets.each{|s| s.close rescue nil} if @sockets && @sockets_created
      @to_child.values.each{|s| s.close rescue nil}
      @to_child.clear
      Timeout.timeout(1){wait_all_children} rescue Thread.new{wait_all_children}
    end

    # @overload reload(host=nil, port, opts={})
    #   @param host [String] hostname or IP address
    #   @param port [Integer / String] port number / service name
    #   @macro args
    # @overload reload(socket, opts={})
    #   @param socket [Socket] listening socket
    #   @macro args
    # @overload reload(sockets, opts={})
    #   @param sockets [Array<Socket>] listening sockets
    #   @macro args
    # @return [void]
    def reload(*args)
      @reload_args = parse_args(*args)
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

    # @return [void]
    def detach_children
      t = Time.now + 5
      talk_to_children(detach: true)
      while Time.now < t && @child_status.values.any?{|s| s[:status] == :run}
        watch_children
      end
    end

    private

    # @return [void]
    def do_reload
      sockets, host, port, @opts = @reload_args
      @reload_args = nil
      old_listen_backlog = @listen_backlog
      set_variables_from_opts

      if @sockets_created ? (@host != host || @port != port) : @sockets != sockets
        @sockets.each{|s| s.close rescue nil} if @sockets_created
        detach_children
        @sockets, @host, @port = sockets, host, port
        if @sockets
          @sockets_created = false
        else
          @sockets = create_server_socket(@host, @port, @listen_backlog)
          @sockets_created = true
        end
      elsif @listen_backlog != old_listen_backlog
        @sockets.each{|s| s.listen(@listen_backlog)} if @listen_backlog && @sockets_created
      end

      reload_children
    end

    # @param host [String] hostname or IP address
    # @param port [Integer / String] port number / service name
    # @param backlog [Integer / nil] listen backlog
    # @return [Array<Socket>] listening sockets
    def create_server_socket(host, port, backlog)
      t = Time.now + 5
      begin
        sockets = Socket.tcp_server_sockets(host, port)
      rescue Errno::EADDRINUSE
        raise if Time.now > t
        sleep 0.1
        retry
      end
      sockets.each{|s| s.listen(backlog)} if backlog
      sockets
    end

    # @return [void]
    def reload_children
      data = {}
      data[:options] = @opts.select{|_, value| Marshal.dump(value) rescue nil}
      talk_to_children data
    end

    # @param data [String]
    # @return [void]
    def talk_to_children(data)
      data_to_child = Marshal.dump(data)
      each_nonblock(@to_child.values, 1) do |io|
        Conversation._send(io, data_to_child) rescue nil
      end
    end

    # @param values [Array]
    # @param timeout [Numeric]
    # @yield [obj]
    # @yieldparam obj [Object] one of values
    def each_nonblock(values, timeout)
      values = values.dup
      until values.empty?
        thr = Thread.new do
          until values.empty? || Thread.current[:exit]
            value = values.shift
            break unless value
            yield value
          end
        end
        thr.join(timeout)
        thr[:exit] = true
      end
    end

    # @overload parse_args(host=nil, port, opts={})
    #   @param host [String] hostname or IP address
    #   @param port [Integer / String] port number / service name
    #   @macro args
    # @overload parse_args(socket, opts={})
    #   @param socket [Socket] listening socket
    #   @macro args
    # @overload parse_args(sockets, opts={})
    #   @param sockets [Array<Socket>] listening sockets
    #   @macro args
    # @return [Array<Array<Socket>, String, String, Hash>] sockets, hostname, port, option.
    #   either sockets or (hostname & port) is available.
    def parse_args(*args)
      opts = {}
      arg_count = args.size
      if args.last.is_a? Hash
        opts = args.pop
      end
      if args.size == 1
        case args.first
        when Integer, String
          host, port = nil, args.first
        else
          sockets = [args.first].flatten
        end
      elsif args.size == 2
        host, port = args
      else
        raise ArgumentError, "wrong number of arguments (#{arg_count} for 1..3)"
      end
      return sockets, host, port, opts
    end

    # @return [Integer]
    def watch_children
      rset = @from_child.empty? ? nil : @from_child.keys
      readable, = IO.select(rset, nil, nil, 0.1)
      if readable
        readable.each do |from_child|
          if st = Conversation.recv(from_child)
            st[:time] = Time.now
            @child_status[from_child].update st
          else
            @from_child.delete from_child
            @to_child[from_child].close rescue nil
            @to_child.delete from_child
            @child_status.delete from_child
            from_child.close
          end
        end
      end
      kill_frozen_children
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
    def kill_frozen_children
      now = Time.now
      @child_status.each do |r, st|
        if now > st[:time] + @watchdog_timer + 60
          Process.kill 'KILL', @from_child[r] rescue nil
        elsif now > st[:time] + @watchdog_timer && ! st[:signal_sent]
          Process.kill @watchdog_signal, @from_child[r] rescue nil
          st[:signal_sent] = true
        end
      end
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
        @from_child.keys.each{|p| p.close rescue nil}
        @to_child.values.each{|p| p.close rescue nil}
        from_child[0].close
        to_child[1].close
        @on_start.call if @on_start
        Child.new(@sockets, @opts, from_child[1], to_child[0]).start(@block)
        exit! true
      end
      from_child[1].close
      to_child[0].close
      r, w = from_child[0], to_child[1]
      @from_child[r] = pid
      @to_child[r] = w
      @child_status[r] = {status: :run, connections: {}, time: Time.now}
      @children.push pid
      @on_child_start.call(pid) if @on_child_start
    end

    def set_variables_from_opts
      @min_processes = @opts[:min_processes] || DEFAULT_MIN_PROCESSES
      @max_processes = @opts[:max_processes] || DEFAULT_MAX_PROCESSES
      @max_threads = @opts[:max_threads] || DEFAULT_MAX_THREADS
      @standby_threads = @opts[:standby_threads] || DEFAULT_STANDBY_THREADS
      @listen_backlog = @opts[:listen_backlog]
      @watchdog_timer = @opts[:watchdog_timer] || DEFAULT_WATCHDOG_TIMER
      @watchdog_signal = @opts[:watchdog_signal] || DEFAULT_WATCHDOG_SIGNAL
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

      # @return [Integer]
      def max_use
        @options[:max_use] || DEFAULT_MAX_USE
      end

      # @param block [#call]
      # @return [void]
      def start(block)
        queue = Queue.new
        accept_thread = Thread.new{ accept_loop(block, queue) }
        reload_thread = Thread.new{ reload_loop(queue) }

        # wait that accept_loop or reload_loop end
        queue.pop

        accept_thread.exit
        @sockets.each{|s| s.close rescue nil}
        @threads_mutex.synchronize do
          notify_status
        end
        wait_all_connections
        reload_thread.exit
      end

      private

      # @param block [#call]
      # @param queue [Queue]
      # @return [void]
      def accept_loop(block, queue)
        count = 0
        while @status == :run
          wait_thread
          sock, addr = accept
          next if sock.nil? && count == 0
          break unless sock
          thr = Thread.new(sock, addr){|s, a| run(s, a, block)}
          @threads_mutex.synchronize do
            @threads[thr] = addr
          end
          count += 1
          break if max_use > 0 && count >= max_use
        end
      rescue => e
        STDERR.puts e.inspect, e.backtrace.inspect
        raise e
      ensure
        @status = :stop
        queue.push true
      end

      # @return [void]
      def wait_all_connections
        @threads.keys.each do |thr|
          thr.join rescue nil
        end
        @status = :exit
      end

      # @param queue [Queue]
      # @return [void]
      def reload_loop(queue)
        heartbeat_interval = 5
        while true
          time = Time.now
          if IO.select([@from_parent], nil, nil, heartbeat_interval)
            heartbeat_interval -= Time.now - time
            heartbeat_interval = 0 if heartbeat_interval < 0
            data = Conversation.recv(@from_parent)
            break if data.nil? or data[:detach]
            @options.update data[:options] if data[:options]
            @options[:on_reload].call @options if @options[:on_reload]
            @threads_cv.signal
          else
            heartbeat_interval = 5
            @threads_mutex.synchronize do
              Conversation.send(@to_parent, {})
            end
          end
        end
        @from_parent.close
        @from_parent = nil
      rescue => e
        STDERR.puts e.inspect, e.backtrace.inspect
        raise e
      ensure
        @status = :stop
        queue.push true
      end

      # @return [void]
      def wait_thread
        @threads_mutex.synchronize do
          while true
            @threads.select!{|thr,| thr.alive?}
            break if @threads.size < max_threads
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

      # @return [Array<Socket, AddrInfo>]
      # @return [nil]
      def accept
        while true
          timeout = max_idle > 0 ? max_idle : nil
          readable, = IO.select(@sockets, nil, nil, timeout)
          return nil unless readable
          r, = readable
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
