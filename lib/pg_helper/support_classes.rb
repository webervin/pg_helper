# Main module of PgHelper gem/plugin
module PgHelper
  # Indicates that query returned unexpected columnt count
  class PgHelperErrorInvalidColumnCount < PGError; end

  # Indicates that query returned too much rows
  class PgHelperErrorInvalidRowCount < PGError; end

  # Indicates that transaction was called while inside transaction
  class PgHelperErrorNestedTransactionNotAllowed  < PGError; end

  # For use inside transaction to cause rollback.
  class PgHelperErrorRollback < PGError; end

  # Indicates that call is invalid outside transaction
  class PgHelperErrorInvalidOutsideTransaction < PGError; end

  # Invalid argument
  class PgHelperErrorParamsMustBeArrayOfStrings < PGError; end

  # data validation
  class ValidationHelper
    class << self
      def require_single_row!(pg_result)
        fail(
          PgHelperErrorInvalidRowCount,
          "expected 1 row, got #{pg_result.ntuples}"
        ) if pg_result.ntuples != 1
      end

      def require_single_column!(pg_result)
        fail(
          PgHelperErrorInvalidColumnCount,
          "expected 1 column, got #{pg_result.nfields}"
        ) if pg_result.nfields != 1
      end

      def verify_single_cell!(pg_result)
        require_single_row!(pg_result)
        require_single_column!(pg_result)
      end
    end
  end
end
