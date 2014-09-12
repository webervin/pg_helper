# This file is simplified version of ActiveRecord::Base::ConnectionPool
# (from Ruby on Rails framework).
#
# Original code is:
# https://github.com/rails
# Distributed under MIT license.
#
# Rails is developed and maintained by
# David Heinemeier Hansson
# http://www.rubyonrails.org
#
# Please note that as code is modified by me Ruby on Rails team is not responsible for any bugs,
# bugs are added by me :)
# Also note that pg_helper is NOT related to Ruby on Rails in any way

require 'thread'
require 'thread_safe'
require 'monitor'
require 'set'
require 'pg'

module PgHelper
  # Raised when a connection could not be obtained within the connection
  # acquisition timeout period: because max connections in pool
  # are in use.
  class CouldNotObtainConnection < RuntimeError
  end

  class ConnectionPool
    # nearly standard queue, but with timeout on wait
    # FIXME: custom class inherit from
    # http://www.ruby-doc.org/stdlib-2.0/libdoc/thread/rdoc/Queue.html
    class Queue
      def initialize(lock = Monitor.new)
        @lock = lock
        @cond = @lock.new_cond
        @num_waiting = 0
        @queue = []
      end

      # Test if any threads are currently waiting on the queue.
      def any_waiting?
        synchronize do
          @num_waiting > 0
        end
      end

      # Return the number of threads currently waiting on this
      # queue.
      def num_waiting
        synchronize do
          $DEBUG && warn(@num_waiting.to_s)
          @num_waiting
        end
      end

      # Add +element+ to the queue.  Never blocks.
      def add(element)
        synchronize do
          @queue.push element
          @cond.signal
        end
      end

      # If +element+ is in the queue, remove and return it, or nil.
      def delete(element)
        synchronize do
          @queue.delete(element)
        end
      end

      # Remove all elements from the queue.
      def clear
        synchronize do
          @queue.clear
        end
      end

      # Remove the head of the queue.
      #
      # If +timeout+ is not given, remove and return the head the
      # queue if the number of available elements is strictly
      # greater than the number of threads currently waiting (that
      # is, don't jump ahead in line).  Otherwise, return nil.
      #
      # If +timeout+ is given, block if it there is no element
      # available, waiting up to +timeout+ seconds for an element to
      # become available.
      #
      # Raises:
      # - ConnectionTimeoutError if +timeout+ is given and no element
      # becomes available after +timeout+ seconds,
      def poll(timeout = nil)
        synchronize do
          if timeout
            no_wait_poll || wait_poll(timeout)
          else
            no_wait_poll
          end
        end
      end

      private

      def synchronize(&block)
        @lock.synchronize(&block)
      end

      # Test if the queue currently contains any elements.
      def any?
        !@queue.empty?
      end

      # A thread can remove an element from the queue without
      # waiting if an only if the number of currently available
      # connections is strictly greater than the number of waiting
      # threads.
      def can_remove_no_wait?
        @queue.size > @num_waiting
      end

      # Removes and returns the head of the queue if possible, or nil.
      def remove
        @queue.shift
      end

      # Remove and return the head the queue if the number of
      # available elements is strictly greater than the number of
      # threads currently waiting.  Otherwise, return nil.
      def no_wait_poll
        remove if can_remove_no_wait?
      end

      # Waits on the queue up to +timeout+ seconds, then removes and
      # returns the head of the queue.
      def wait_poll(timeout)
        @num_waiting += 1

        t0 = Time.now
        elapsed = 0
        loop do
          @cond.wait(timeout - elapsed)

          return remove if any?

          elapsed = Time.now - t0
          if elapsed >= timeout
            msg = 'could not obtain a database connection within %0.3f seconds (waited %0.3f seconds)' %
              [timeout, elapsed]
            fail CouldNotObtainConnection, msg
          end
        end
      ensure
        @num_waiting -= 1
      end
    end

    include MonitorMixin

    attr_accessor :auto_connect, :checkout_timeout
    attr_reader :connection_options, :connections, :size

    # all opts  but :checkout_timeout, :pool, :auto_connect will be passed to PGConn.new
    def initialize(opts)
      super()

      connection_opts = opts.dup
      @checkout_timeout = opts.delete(:checkout_timeout) || 5
      @size = opts.delete(:pool) || 5
      @auto_connect = opts.delete(:auto_connect) || true

      @connection_options = connection_opts

      # The cache of reserved connections mapped to threads
      @reserved_connections = ThreadSafe::Cache.new(initial_capacity: @size)

      @connections         = []

      @available = Queue.new self
    end

    # Retrieve the connection associated with the current thread, or call
    # #checkout to obtain one if necessary.
    #
    # #connection can be called any number of times; the connection is
    # held in a hash keyed by the thread id.
    def connection
      # this is correctly done double-checked locking
      # (ThreadSafe::Cache's lookups have volatile semantics)
      @reserved_connections[current_connection_id] || synchronize do
        @reserved_connections[current_connection_id] ||= checkout
      end
    end

    # Is there an open connection that is being used for the current thread?
    def active_connection?
      synchronize do
        @reserved_connections.fetch(current_connection_id) do
          return false
        end
      end
    end

    # Signal that the thread is finished with the current connection.
    # #release_connection releases the connection-thread association
    # and returns the connection to the pool.
    def release_connection(with_id = current_connection_id)
      synchronize do
        conn = @reserved_connections.delete(with_id)
        checkin conn if conn
      end
    end

    # If a connection already exists yield it to the block. If no connection
    # exists checkout a connection, yield it to the block, and checkin the
    # connection when finished.
    def with_connection
      connection_id = current_connection_id
      fresh_connection = true unless active_connection?
      yield connection
    ensure
      release_connection(connection_id) if fresh_connection
    end

    # Returns true if a connection has already been opened.
    def connected?
      synchronize { @connections.any? }
    end

    # Disconnects all connections in the pool, and clears the pool.
    def disconnect!
      synchronize do
        @reserved_connections.clear
        @connections.each do |conn|
          checkin conn
          $DEBUG && warn("Closing pg connection: #{conn.object_id}")
          conn.close
        end
        @connections = []
        @available.clear
      end
    end

    # Clears the cache which maps classes.
    def clear_reloadable_connections!
      synchronize do
        @reserved_connections.clear
        @connections.each do |conn|
          checkin conn
        end

        @connections.delete_if(&:finished?)

        @available.clear
        @connections.each do |conn|
          @available.add conn
        end
      end
    end

    # Check-out a database connection from the pool, indicating that you want
    # to use it. You should call #checkin when you no longer need this.
    #
    # This is done by either returning and leasing existing connection, or by
    # creating a new connection and leasing it.
    #
    # If all connections are leased and the pool is at capacity (meaning the
    # number of currently leased connections is greater than or equal to the
    # size limit set), an ActiveRecord::ConnectionTimeoutError exception will be raised.
    #
    # Returns: an AbstractAdapter object.
    #
    # Raises:
    # - ConnectionTimeoutError: no connection can be obtained from the pool.
    def checkout
      synchronize do
        conn = acquire_connection
        checkout_and_verify(conn)
      end
    end

    # Check-in a database connection back into the pool, indicating that you
    # no longer need this connection.
    #
    # +conn+: an AbstractAdapter object, which was obtained by earlier by
    # calling +checkout+ on this pool.
    def checkin(conn)
      synchronize do
        release conn
        @available.add conn
      end
    end
    #       # Remove a connection from the connection pool.  The connection will
    #       # remain open and active but will no longer be managed by this pool.
    #       def remove(conn)
    #         synchronize do
    #           @connections.delete conn
    #           @available.delete conn
    #
    #           # FIXME: we might want to store the key on the connection so that removing
    #           # from the reserved hash will be a little easier.
    #           release conn
    #
    #           @available.add checkout_new_connection if @available.any_waiting?
    #         end
    #       end

    private

    # Acquire a connection by one of 1) immediately removing one
    # from the queue of available connections, 2) creating a new
    # connection if the pool is not at capacity, 3) waiting on the
    # queue for a connection to become available.
    #
    # Raises:
    # - ConnectionTimeoutError if a connection could not be acquired
    def acquire_connection
      if conn = @available.poll
        conn
      elsif @connections.size < @size
        checkout_new_connection
      else
        @available.poll(@checkout_timeout)
      end
    end

    def release(conn)
      thread_id = if @reserved_connections[current_connection_id] == conn
                    current_connection_id
      else
        @reserved_connections.keys.find do |k|
          @reserved_connections[k] == conn
        end
      end

      @reserved_connections.delete thread_id if thread_id
    end

    def new_connection
      conn = PGconn.open(connection_options)
      $DEBUG && warn("Connected to PostgreSQL #{conn.server_version} (#{conn.object_id})")
      conn
    end

    def current_connection_id #:nodoc:
      Thread.current.object_id
    end

    def checkout_new_connection
      fail ConnectionNotEstablished unless @auto_connect
      c = new_connection
      @connections << c
      c
    end

    def checkout_and_verify(c)
      c.reset
      c
    end
  end
end
