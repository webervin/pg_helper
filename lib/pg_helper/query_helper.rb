# Main module of PgHelper gem/plugin
module PgHelper
  # Main api class
  class QueryHelper
    # @return [Hash]  connection params
    attr_accessor :connection_params

    # Active database connection
    # @return [PGconn]  connection see {http://rubygems.org/gems/pg gem}
    attr_accessor :pg_connection

    # Creates a new instance of  the QueryHelper
    def self.using_pool(pool, &_block)
      helper = nil
      pool.with_connection do |conn|
        helper = new(conn)
        yield helper
      end
    ensure
      helper = nil
    end

    def initialize(params)
      if params.is_a? PG::Connection
        @pg_connection = params
        @connection_params = nil
      else
        @connection_params = params
        reconnect
      end
    end

    # @param [String]  query SQL select that should return one cell,
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return [String]
    def value(query, params = [])
      exec(query, params) do |pg_result|
        ValidationHelper.verify_single_cell!(pg_result)
        pg_result.getvalue(0, 0)
      end
    end

    # @param [String]  query SQL select that should return one column,
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return [Array<String>]  Values of selected column
    def get_column(query, params = [])
      exec(query, params) do |pg_result|
        ValidationHelper.require_single_column!(pg_result)
        pg_result.column_values(0)
      end
    end

    # @param [String]  query SQL select that should return one row,
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return [Hash]  Hash of column_name => row_value for resulting row
    def get_hash(query, params = [])
      exec(query, params) do |pg_result|
        ValidationHelper.require_single_row!(pg_result)
        pg_result[0]
      end
    end

    # @param [String]  query SQL select,
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return [Array<Array>]  Array containing Array of values for each row
    def get_all(query, params = [])
      exec(query, params) do |pg_result|
        pg_result.values
      end
    end

    # @param [String]  query SQL select,
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return [Array<Hash>]  Array of row hashes column_name => row_value
    def get_all_hashes(query, params = [])
      exec(query, params) do |pg_result|
        pg_result.to_a
      end
    end

    # @param [String]  query SQL select
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return String csv representation of query result with csv header
    def csv(query, params = [])
      csv_query = "COPY (#{query}) TO STDOUT with CSV HEADER"
      exec(csv_query, params) do
        csv_data = ''
        buf = @pg_connection.get_copy_data(true)
        while buf
          csv_data += buf
          buf = @pg_connection.get_copy_data(true)
        end
        csv_data
      end
    end

    # @param [String]  query SQL update,
    # may include $1, $2 etc to be replaced by query arguments
    # @param [Array<String>]  params query arguments
    # @return [Integer]  Number of rows changed
    def modify(query, params = [])
      exec(query, params) do |pg_result|
        pg_result.cmd_tuples
      end
    end

    # Executes content of given block inside database transaction
    # @yield [QueryHelper]
    def transaction(&block)
      verify_transaction_possible!(&block)
      perform_transaction(&block)
    end

    # Aborts current transaction, or raises exception if invoked
    # outside transaction.
    # @return [void]
    def rollback!
      fail PgHelperErrorInvalidOutsideTransaction if connection_idle?
      fail PgHelperErrorRollback
    end

    protected

    def connection_idle?
      PG::Constants::PQTRANS_IDLE == @pg_connection.transaction_status
    end

    def exec(query, params = [], &block)
      check_query_params(params)
      pg_result = nil
      begin
        pg_result = @pg_connection.exec(query, params)
        block.call(pg_result)
      ensure
        pg_result && pg_result.clear
      end
    end

    def check_query_params(params)
      params.is_a?(Array) || fail(PgHelperErrorParamsMustBeArrayOfStrings)
    end

    def reconnect
      @pg_connection = PG::Connection.open(@connection_params)  if @connection_params
    end

    def perform_transaction(&block)
      @pg_connection.transaction do
        begin
          block.call(self)
          rescue PgHelperErrorRollback
            true
        end
      end
    end

    def verify_transaction_possible!(&_block)
      fail PgHelperErrorNestedTransactionNotAllowed unless connection_idle?
      fail ArgumentError, 'missing block' unless block_given?
    end
  end
end
