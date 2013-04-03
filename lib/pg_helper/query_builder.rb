module PgHelper
  class QueryBuilder
    attr_reader :table_name

    def self.from(table_name)
      self.new(table_name)
    end

    def initialize(table_name)
      @table_name = table_name
      @selects = []
      @where = []
      @cte_list = []
      @join_list = []
    end

    # http://www.postgresql.org/docs/9.2/static/sql-select.html
    def to_sql
      "#{with_list}SELECT #{column_list} FROM #{table_name}#{join_list}#{where_list}"
    end

    def select(value)
      @selects << value
      self
    end

    def where(condition)
      @where << condition
      self
    end

    def with(cte_name, cte_query)
      @cte_list << "#{cte_name} AS (#{cte_query})"
      self
    end

    def join(join_sql)
      @join_list << join_sql
      self
    end

    private
      def join_list
        if @join_list.empty?
          nil
        else
          " #{@join_list.join(' ')}"
        end
      end

      def with_list
        if @cte_list.empty?
          nil
        else
          "WITH #{@cte_list.join(',')} "
        end
      end

      def where_list
        if @where.empty?
          nil
        else
          " WHERE #{@where.join(' AND ')}"
        end
      end


      def column_list
        if @selects.empty?
          '*'
        else
          @selects.join(',')
        end
      end
  end
end