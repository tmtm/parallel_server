require 'test/unit'
require 'test/unit/notify'
require 'socket'
require 'timeout'
require 'tempfile'

require 'parallel_server/prefork'

class TestParallelServerPrefork < Test::Unit::TestCase
  def setup
    @tmpf = Tempfile.new('test_prefork')
    @prefork = ParallelServer::Prefork.new(port, opts)
    @thr = Thread.new do
      begin
        @prefork.start do |sock|
          begin
            sock.puts $$.to_s
            sock.gets
          rescue Errno::EPIPE
          end
        end
      rescue
        p $!, $@
        raise
      end
    end
    sleep 1
    @clients = []
  end

  def teardown
    @clients.each do |c|
      c.close rescue nil
    end
    @prefork.stop!
    @thr.join rescue nil
    @tmpf.close true
  end

  def port
    ENV['TEST_PORT'] || 12345
  end

  def connect
    s = TCPSocket.new('localhost', port)
    @clients.push s
    s.gets
    s
  end

  sub_test_case 'max_process: 1, max_threads: 1' do
    def opts
      {
        min_processes: 1,
        max_processes: 1,
        max_threads: 1,
      }
    end

    test '2nd wait for connection' do
      connect
      assert_raise Timeout::Error do
        Timeout.timeout(0.5){ connect }
      end
    end

    test '2nd connect after 1st connection is closed' do
      connect
      Thread.new{ sleep 0.3; @clients.first.puts }
      Timeout.timeout(1){ connect }
    end
  end

  sub_test_case 'max_process: 1, max_threads: 3' do
    def opts
      {
        min_processes: 1,
        max_processes: 1,
        max_threads: 3,
      }
    end

    test '4th request wait for connection' do
      3.times{ connect }
      assert_raise Timeout::Error do
        Timeout.timeout(0.5){ connect }
      end
    end

    test '4th connect after 1st connection is closed' do
      3.times{ connect }
      Thread.new{ sleep 0.3; @clients.first.puts }
      Timeout.timeout(1){ connect }
    end
  end

  sub_test_case 'max_process: 3, max_threads: 1' do
    def opts
      {
        min_processes: 1,
        max_processes: 3,
        max_threads: 1,
      }
    end

    test '4th request wait for connection' do
      3.times{ connect }
      assert_raise Timeout::Error do
        Timeout.timeout(0.5){ connect }
      end
    end

    test '4th connect after 1st connection is closed' do
      3.times{ connect }
      Thread.new{ sleep 0.3; @clients.first.puts }
      Timeout.timeout(1){ connect }
    end
  end

  sub_test_case 'min_processes: 3' do
    def opts
      {
        min_processes: 3,
        standby_threads: 1,
      }
    end

    test '3 child processes exist' do
      c = Dir.glob('/proc/*/status').count do |stat|
        File.read(stat) =~ /^PPid:\t#{$$}$/
      end
      assert_equal 3, c
    end
  end

  sub_test_case 'max_idle' do
    def opts
      @children = []
      {
        min_processes: 1,
        max_processes: 1,
        max_idle: 0.1,
        on_child_start: ->(pid){ @children.push pid },
      }
    end

    test 'unconnected process exists even if max_idle expired' do
      sleep 0.5
      assert File.exist?("/proc/#{@children[0]}")
      assert_equal 1, @children.size
    end

    test 'first child process exited if it is connected' do
      Process.waitpid fork{ connect.close }
      sleep 0.5
      assert ! File.exist?("/proc/#{@children[0]}")
      assert_equal 2, @children.size
    end
  end

  sub_test_case 'max_use' do
    def opts
      @children = []
      {
        min_processes: 1,
        max_processes: 1,
        max_use: 2,
        on_child_start: ->(pid){ @children.push pid },
      }
    end

    test 'child process exited if it is connected max_use times' do
      Process.waitpid fork{ connect.close }
      sleep 0.5
      assert File.exist?("/proc/#{@children[0]}")
      Process.waitpid fork{ connect.close }
      sleep 0.5
      assert ! File.exist?("/proc/#{@children[0]}")
    end
  end

  sub_test_case 'standby_threads' do
    def opts
      @children = []
      {
        min_processes: 1,
        max_processes: 20,
        max_threads: 2,
        on_child_start: ->(pid){ @children.push pid }
      }
    end

    sub_test_case 'standby_threads=1' do
      def opts
        {
          standby_threads: 1
        }.merge super
      end

      test '1 child start' do
        assert_equal 1, @children.size
      end
    end

    sub_test_case '1 < standby_threads <= max_processes/max_threads' do
      def opts
        {
          standby_threads: 10
        }.merge super
      end

      test 'n children start' do
        assert_equal 5, @children.size
      end
    end

    sub_test_case 'standby_threads > max_processes/max_threads' do
      def opts
        {
          standby_threads: 100
        }.merge super
      end

      test 'max_processes children start' do
        assert_equal 20, @children.size
      end
    end
  end

  sub_test_case 'on_start' do
    def opts
      {
        min_processes: 1,
        max_processes: 2,
        max_threads: 1,
        standby_threads: 1,
        on_start: ->(){ File.open(@tmpf.path, 'a'){|f| f.puts $$} },
      }
    end

    test 'execute block in child process when child start' do
      assert_equal 1, File.read(@tmpf.path).lines.count
      connect.close
      sleep 0.5
      assert_equal 2, File.read(@tmpf.path).lines.count
    end
  end

  sub_test_case 'on_reload' do
    def opts
      {
        min_processes: 5,
        max_processes: 5,
        on_reload: ->(_opts){ File.open(@tmpf.path, 'a'){|f| f.puts $$} },
      }
    end

    test 'execute block when reload' do
      assert_equal 0, File.read(@tmpf.path).lines.count
      @prefork.reload(12345, opts)
      sleep 0.5
      assert_equal 5, File.read(@tmpf.path).lines.count
    end
  end

  sub_test_case 'on_child_start' do
    def opts
      @childs = []
      {
        min_processes: 1,
        max_processes: 2,
        max_threads: 1,
        standby_threads: 1,
        on_child_start: ->(pid){ @childs.push pid },
      }
    end

    test 'execute block when child start' do
      assert_equal 1, @childs.count
      connect.close
      sleep 0.5
      assert_equal 2, @childs.count
    end
  end

  sub_test_case 'on_child_exit' do
    def opts
      @childs = []
      {
        min_processes: 1,
        max_processes: 1,
        max_idle: 0,
        on_child_exit: ->(pid, st){ @childs.push pid; @st = st },
      }
    end

    test 'execute block when child exit' do
      assert_equal [], @childs
      s = TCPSocket.new('localhost', port)
      pid = s.gets.chomp.to_i
      s.close
      sleep 0.5
      assert_equal [pid], @childs
      assert_equal 0, @st.exitstatus
    end
  end
end
