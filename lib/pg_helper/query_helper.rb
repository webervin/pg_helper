#Main module of PgHelper gem/plugin
module PgHelper

#Indicates that query returned unexpected columnt count
class PgHelperErrorInvalidColumnCount < PGError; end

#Indicates that query returned too much rows
class PgHelperErrorInvalidRowCount < PGError; end

# Indicates that transaction was called while inside transaction
class PgHelperErrorNestedTransactionNotAllowed  < PGError; end

#For use inside transaction to cause rollback.
class PgHelperErrorRollback < PGError; end

#Indicates that call is invalid outside transaction
class PgHelperErrorInvalidOutsideTransaction < PGError; end

#Invalid argument
class PgHelperErrorParamsMustBeArrayOfStrings < PGError; end

#Main api class
class QueryHelper

  # @return [Hash]  connection params
  attr_accessor :connection_params

  # Active database connection
  # @return [PGconn]  connection see {http://rubygems.org/gems/pg Pg gem on rubygems} for details
  attr_accessor :pg_connection


  # Creates a new instance of  the QueryHelper
  def self.using_pool(pool, &block)
    helper = nil
    pool.with_connection do |conn|
      helper = self.new(conn)
      yield helper
    end
  ensure
    helper = nil
  end

  def initialize(params)
    if params.kind_of? PGconn
      @pg_connection = params
      @connection_params = nil
    else
      @connection_params = params
      reconnect
    end
  end

  # @param [String]  query SQL select that should return one cell, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return [String]
  def value(query, params = [])
    exec(query, params) do |pg_result|
      verify_single_cell!(pg_result)
      pg_result.getvalue(0,0)
    end
  end

  # @param [String]  query SQL select that should return one column, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return [Array<String>]  Values of selected column
  def get_column(query, params = [])
    exec(query, params) do |pg_result|
      require_single_column!(pg_result)
      pg_result.column_values(0)
    end
  end

  # @param [String]  query SQL select that should return one row, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return [Hash]  Hash of column_name => row_value for resulting row
  def get_hash(query, params = [])
    exec(query, params) do |pg_result|
      require_single_row!(pg_result)
      pg_result.res[0]
    end
  end

  # @param [String]  query SQL select, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return [Array<Array>]  Array containing Array of values for each row
  def get_all(query, params = [])
    exec(query, params) do |pg_result|
      pg_result.values
    end
  end

  # @param [String]  query SQL select, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return [Array<Hash>]  Array containing hash of column_name => row_value for each row
  def get_all_hashes(query, params = [])
    exec(query, params) do |pg_result|
      pg_result.to_a
    end
  end

  # @param [String]  query SQL select, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return String csv representation of query result with csv header
  def csv(query)
    csv_query = "COPY (#{query}) TO STDOUT with CSV HEADER"
    exec(csv_query, params = []) do
      csv_data = ""
      csv_data += buf while buf = @pg_connection.get_copy_data(true)
      csv_data
    end
  end

  # @param [String]  query SQL update, may include $1, $2 etc to be replaced by arguments
  # @param [Array<String>]  params query arguments to be passed on to PostgreSql
  # @return [Integer]  Number of rows changed
  def modify(query, params = [])
    exec(query, params) do |pg_result|
      pg_result.cmd_tuples
    end
  end

  # Executes content of given block inside database transaction
  #@yield [QueryHelper] 
  def transaction(&block)
    verify_transaction_possible!(&block)
    perform_transaction(&block)
  end

  # Aborts current transaction, or raises exception if invoked outside transaction.
  #@return [void]
  def rollback!
    raise PgHelperErrorInvalidOutsideTransaction if connection_idle?
    raise PgHelperErrorRollback.new
  end

  protected

  def connection_idle?
    PGconn::PQTRANS_IDLE == @pg_connection.transaction_status
  end

  def require_single_row!(pg_result)
    raise PgHelperErrorInvalidRowCount.new if pg_result.ntuples != 1
  end

  def require_single_column!(pg_result)
    raise PgHelperErrorInvalidColumnCount.new if pg_result.nfields != 1
  end

  def exec(query, params=[], &block)
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
    raise PgHelperErrorParamsMustBeArrayOfStrings.new unless params.is_a?(Array)
  end
  def reconnect
    @pg_connection = PGconn.open(@connection_params)  if @connection_params
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

  def verify_transaction_possible!(&block)
    raise PgHelperErrorNestedTransactionNotAllowed.new unless connection_idle?
    raise ArgumentError.new('missing block') unless block_given?
  end

  def verify_single_cell!(pg_result)
    require_single_row!(pg_result)
    require_single_column!(pg_result)
  end
end
end