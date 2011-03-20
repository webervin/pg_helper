
module PgHelper
class PgHelperErrorInvalidColumnCount < PGError; end
class PgHelperErrorInvalidRowCount < PGError; end
class PgHelperErrorNestedTransactionNotAllowed  < PGError; end
class PgHelperErrorRollback < PGError; end
class PgHelperErrorInvalidOutsideTransaction < PGError; end
class PgHelperErrorParamsMustBeArrayOfStrings < PGError; end

class QueryHelper
  attr_accessor :connection_params, :pg_connection

  def initialize(params)
    @connection_params = params
    reconnect
  end

  def value(query, params = [])
    exec(query, params) do |pg_result|
      verify_single_cell!(pg_result)
      pg_result.getvalue(0,0)
    end
  end

  def get_column(query, params = [])
    exec(query, params) do |pg_result|
      require_single_column!(pg_result)
      pg_result.column_values(0)
    end
  end

  def modify(query, params = [])
    exec(query, params) do |pg_result|
      pg_result.cmd_tuples
    end
  end

  def transaction(&block)
    verify_transaction_possible!(&block)
    perform_transaction(&block)
  end

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
    @pg_connection = PGconn.open(@connection_params)
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