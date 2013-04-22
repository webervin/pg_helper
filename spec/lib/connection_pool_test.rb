
module ActiveRecord
  module ConnectionAdapters
    class ConnectionPoolTest < ActiveRecord::TestCase
      attr_reader :pool

      def setup
        super

        # Keep a duplicate pool so we do not bother others
        @pool = ConnectionPool.new ActiveRecord::Base.connection_pool.spec

        if in_memory_db?
          # Separate connections to an in-memory database create an entirely new database,
          # with an empty schema etc, so we just stub out this schema on the fly.
          @pool.with_connection do |connection|
            connection.create_table :posts do |t|
              t.integer :cololumn
            end
          end
        end
      end

      def teardown
        super
        @pool.disconnect!
      end

      def active_connections(pool)
        pool.connections.find_all(&:in_use?)
      end

      def test_checkout_after_close
        connection = pool.connection
        assert connection.in_use?

        connection.close
        assert !connection.in_use?

        assert pool.connection.in_use?
      end

      def test_released_connection_moves_between_threads
        thread_conn = nil

        Thread.new {
          pool.with_connection do |conn|
            thread_conn = conn
          end
        }.join

        assert thread_conn

        Thread.new {
          pool.with_connection do |conn|
            assert_equal thread_conn, conn
          end
        }.join
      end

      def test_with_connection
        assert_equal 0, active_connections(pool).size

        main_thread = pool.connection
        assert_equal 1, active_connections(pool).size

        Thread.new {
          pool.with_connection do |conn|
            assert conn
            assert_equal 2, active_connections(pool).size
          end
          assert_equal 1, active_connections(pool).size
        }.join

        main_thread.close
        assert_equal 0, active_connections(pool).size
      end

      def test_active_connection_in_use
        assert !pool.active_connection?
        main_thread = pool.connection

        assert pool.active_connection?

        main_thread.close

        assert !pool.active_connection?
      end

      def test_full_pool_exception
        assert_raises(CouldNotObtainConnection) do
          (@pool.size + 1).times do
            @pool.checkout
          end
        end
      end

      def test_full_pool_blocks
        cs = @pool.size.times.map { @pool.checkout }
        t = Thread.new { @pool.checkout }

        # make sure our thread is in the timeout section
        Thread.pass until t.status == "sleep"

        connection = cs.first
        connection.close
        assert_equal connection, t.join.value
      end

      def test_removing_releases_latch
        cs = @pool.size.times.map { @pool.checkout }
        t = Thread.new { @pool.checkout }

        # make sure our thread is in the timeout section
        Thread.pass until t.status == "sleep"

        connection = cs.first
        @pool.remove connection
        assert_respond_to t.join.value, :execute
      end

      def test_reap_and_active
        @pool.checkout
        @pool.checkout
        @pool.checkout
        @pool.dead_connection_timeout = 0

        connections = @pool.connections.dup

        @pool.reap

        assert_equal connections.length, @pool.connections.length
      end

      def test_reap_inactive
        @pool.checkout
        @pool.checkout
        @pool.checkout
        @pool.dead_connection_timeout = 0

        connections = @pool.connections.dup
        connections.each do |conn|
          conn.extend(Module.new { def active?; false; end; })
        end

        @pool.reap

        assert_equal 0, @pool.connections.length
      ensure
        connections.each(&:close)
      end

      def test_remove_connection
        conn = @pool.checkout
        assert conn.in_use?

        length = @pool.connections.length
        @pool.remove conn
        assert conn.in_use?
        assert_equal(length - 1, @pool.connections.length)
      ensure
        conn.close
      end

      def test_remove_connection_for_thread
        conn = @pool.connection
        @pool.remove conn
        assert_not_equal(conn, @pool.connection)
      ensure
        conn.close if conn
      end

      def test_active_connection?
        assert !@pool.active_connection?
        assert @pool.connection
        assert @pool.active_connection?
        @pool.release_connection
        assert !@pool.active_connection?
      end

      def test_checkout_behaviour
        pool = ConnectionPool.new ActiveRecord::Base.connection_pool.spec
        connection = pool.connection
        assert_not_nil connection
        threads = []
        4.times do |i|
          threads << Thread.new(i) do
            connection = pool.connection
            assert_not_nil connection
            connection.close
          end
        end

        threads.each(&:join)

        Thread.new do
          assert pool.connection
          pool.connection.close
        end.join
      end

      # The connection pool is "fair" if threads waiting for
      # connections receive them the order in which they began
      # waiting.  This ensures that we don't timeout one HTTP request
      # even while well under capacity in a multi-threaded environment
      # such as a Java servlet container.
      #
      # We don't need strict fairness: if two connections become
      # available at the same time, it's fine of two threads that were
      # waiting acquire the connections out of order.
      #
      # Thus this test prepares waiting threads and then trickles in
      # available connections slowly, ensuring the wakeup order is
      # correct in this case.
      def test_checkout_fairness
        @pool.instance_variable_set(:@size, 10)
        expected = (1..@pool.size).to_a.freeze
        # check out all connections so our threads start out waiting
        conns = expected.map { @pool.checkout }
        mutex = Mutex.new
        order = []
        errors = []

        threads = expected.map do |i|
          t = Thread.new {
            begin
              @pool.checkout # never checked back in
              mutex.synchronize { order << i }
            rescue => e
              mutex.synchronize { errors << e }
            end
          }
          Thread.pass until t.status == "sleep"
          t
        end

        # this should wake up the waiting threads one by one in order
        conns.each { |conn| @pool.checkin(conn); sleep 0.1 }

        threads.each(&:join)

        raise errors.first if errors.any?

        assert_equal(expected, order)
      end

      # As mentioned in #test_checkout_fairness, we don't care about
      # strict fairness.  This test creates two groups of threads:
      # group1 whose members all start waiting before any thread in
      # group2.  Enough connections are checked in to wakeup all
      # group1 threads, and the fact that only group1 and no group2
      # threads acquired a connection is enforced.
      def test_checkout_fairness_by_group
        @pool.instance_variable_set(:@size, 10)
        # take all the connections
        conns = (1..10).map { @pool.checkout }
        mutex = Mutex.new
        successes = []    # threads that successfully got a connection
        errors = []

        make_thread = proc do |i|
          t = Thread.new {
            begin
              @pool.checkout # never checked back in
              mutex.synchronize { successes << i }
            rescue => e
              mutex.synchronize { errors << e }
            end
          }
          Thread.pass until t.status == "sleep"
          t
        end

        # all group1 threads start waiting before any in group2
        group1 = (1..5).map(&make_thread)
        group2 = (6..10).map(&make_thread)

        # checkin n connections back to the pool
        checkin = proc do |n|
          n.times do
            c = conns.pop
            @pool.checkin(c)
          end
        end

        checkin.call(group1.size)         # should wake up all group1

        loop do
          sleep 0.1
          break if mutex.synchronize { (successes.size + errors.size) == group1.size }
        end

        winners = mutex.synchronize { successes.dup }
        checkin.call(group2.size)         # should wake up everyone remaining

        group1.each(&:join)
        group2.each(&:join)

        assert_equal((1..group1.size).to_a, winners.sort)

        if errors.any?
          raise errors.first
        end
      end
    end
  end
end
