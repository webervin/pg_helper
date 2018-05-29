require File.expand_path('../../spec_helper', __FILE__)
include PgHelper

require 'pg'
UNIQUE_TABLE_NAME = "pg_helper_test_#{(rand * 100_000_000).to_i}"
REAL_PARAMS = { host: 'localhost' }
RSpec.describe QueryHelper do
  after(:all) do
    conn = PG::Connection.open(REAL_PARAMS)
    conn.exec("drop table if exists #{UNIQUE_TABLE_NAME} ") { true }
    conn.finish
  end

  describe 'connection params' do
    let(:params) do
      {
        host: 'my host',
        port: 1234,
        username: 'my user',
        password: 'my password',
        dbname: 'my base'
      }
    end

    it 'are stored upon initialization' do
      allow(PG::Connection).to receive(:open).and_return(true)
      expect(QueryHelper.new(params).connection_params).to eq params
    end

    it 'are passed to pgconn' do
      double(:conn).tap do |conn|
        allow(PG::Connection).to receive(:open).with(params).and_return(conn)
        expect(QueryHelper.new(params).pg_connection).to eq conn
      end
    end
  end

  def build_result(params = {})
    double(:result,
           {
             nfields: 1,
             ntuples: 1
           }.merge(params)
      ).as_null_object
  end

  def double_result(params = {}, *args)
    build_result(params).tap do |result_double|
      if args.empty?
        allow(pg_helper.pg_connection)
          .to receive(:exec).and_return(result_double)
      else
        allow(pg_helper.pg_connection)
          .to receive(:exec).with(*args).and_return(result_double)
      end
    end
  end

  def pg_helper
    @pg_helper ||= QueryHelper.new(REAL_PARAMS)
  end

  describe 'single value' do
    it 'is returned as string' do
      expect(pg_helper.value('select 1')).to eq '1'
    end

    it 'raises error if gets non array as params' do
      expect do
        pg_helper.value('select 1', 'bar')
      end.to raise_error(PgHelperErrorParamsMustBeArrayOfStrings)
    end

    it 'allows to pass params to query' do
      param = ["foo; > 'bar'"]
      expect(pg_helper.value('select $1::text', param)).to eq "foo; > 'bar'"
    end

    it 'raises error if more than 1 row returned' do
      expect do
        pg_helper.value('select 1, 2')
      end.to raise_error(PgHelperErrorInvalidColumnCount)
    end

    it 'raises error if more than 1 row returned' do
      expect do
        pg_helper.value('select 1 union select 2')
      end.to raise_error(PgHelperErrorInvalidRowCount)
    end

    it 'clears pg result on success' do
      expect(double_result).to receive(:clear)
      pg_helper.value('select 1')
    end

    it 'clears pg result on failure' do
      expect(double_result(nfields: 2))
        .to receive(:clear).and_return(true)
      expect {
        pg_helper.value('foo')
      }.to raise_error(PgHelper::PgHelperErrorInvalidColumnCount)
    end
  end

  describe 'array of column values' do
    it 'returns values of column as array of stirngs' do
      expect(
        pg_helper.get_column(
          'select 1 union (select 2 union select 3)'
        )
      ).to eq %w(1 2 3)
    end

    it 'raises error if gets non array as params' do
      expect do
        pg_helper.get_column('select 1', 'bar')
      end.to raise_error(PgHelperErrorParamsMustBeArrayOfStrings)
    end

    it 'allows to pass params to query' do
      param = ['foo', ";'bar'"]
      expect(
        pg_helper.get_column(
          'select $1::text as str union select $2::text as str order by str',
          param
        )
      ).to eq [";'bar'", 'foo']
    end

    it 'raises error if more than one column returned' do
      expect do
        pg_helper.get_column('select 1, 2')
      end.to raise_error(PgHelperErrorInvalidColumnCount)
    end

    it 'clears pg result on success' do
      expect(double_result).to receive(:clear)
      pg_helper.get_column('foo')
    end

    it 'clears pg result on failure' do
      lambda do
        expect(double_result(nfields: 2)).to receive(:clear)
        pg_helper.get_column('foo')
      end
    end
  end

  describe 'executing operation' do
    before(:all) do
      helper = pg_helper
      helper.modify(<<-SQL)
        CREATE TABLE IF NOT EXISTS #{UNIQUE_TABLE_NAME}
        (
          test_text text
        )
      SQL
      helper.modify(
        "INSERT INTO #{UNIQUE_TABLE_NAME} (test_text) values ('b')"
      )
    end

    it 'returns number of rows inserted' do
      expect(pg_helper.modify(<<-SQL)).to eq 12
        INSERT INTO #{UNIQUE_TABLE_NAME} (test_text)
        select n::text
        FROM generate_series(1,12,1) as n
      SQL
    end

    it 'returns number of rows updated by query' do
      expect(pg_helper.modify(<<-SQL)).to eq 1
        UPDATE #{UNIQUE_TABLE_NAME} SET test_text = 'c' where test_text = 'b'
      SQL
    end

    it 'uses cmd_tuples of pg_result internally' do
      double(:value).tap do |value|
        double_result(cmd_tuples: value)
        expect(pg_helper.modify('foo')).to eq value
      end
    end

    it 'raises error if gets non array as params' do
      expect do
        pg_helper.modify('select 1', 'bar')
      end.to raise_error(PgHelperErrorParamsMustBeArrayOfStrings)
    end

    it 'allows to pass params to query' do
      sql = 'update foo set test_text =  $1::text'
      double(:value).tap do |result|
        double_result({ cmd_tuples: result }, sql, ['foo'])
        expect(pg_helper.modify(sql, ['foo'])).to eq result
      end
    end

    it 'clears pg result on success' do
      expect(double_result).to receive(:clear)
      pg_helper.modify('foo')
    end

    it 'clears pg result on failure' do
      lambda do
        double_result.tap do |result|
          expect(result).to receive(:cmd_tuples).and_raise(Exception)
          expect(result).to receive(:clear)
        end
        pg_helper.modify('foo')
      end
    end
  end

  describe 'transaction' do
    it 'raises error if no block given' do
      expect { pg_helper.transaction }.to raise_error(ArgumentError)
    end

    it 'allows to rollback' do
      pg_helper.transaction(&:rollback!)
    end

    it 'does not allow rollback if not in transaction' do
      expect { pg_helper.rollback! }
        .to raise_error(PgHelperErrorInvalidOutsideTransaction)
    end

    describe 'using temporary table' do
      def test_table_name
        @test_table_name ||= 'pg_helper_test_' + Time.now.to_i.to_s
      end

      before(:all) do
        sql = <<SQL
        CREATE TABLE #{test_table_name}
        (
          test_text text
        )
SQL
        pg_helper.modify(sql)
      end

      before(:each) do
        pg_helper.modify("delete from #{test_table_name}")
      end

      it 'will rollback on failure' do
        expect do
          pg_helper.transaction do |t|
            t.modify(
              "INSERT INTO #{test_table_name} "\
              "VALUES ('one'), ('two'), ('three')"
            )
            expect(
              t.value("SELECT COUNT(*) FROM #{test_table_name}")
            ).to eq '3'
            fail Exception, 'roll it back'
          end
        end.to raise_error('roll it back')

        expect(
          pg_helper.value("SELECT COUNT(*) FROM #{test_table_name}")
        ).to eq '0'
      end

      it 'will commit in the end' do
        pg_helper.transaction do |t|
          t.modify("INSERT INTO #{test_table_name} VALUES ('pass')")
          t.modify("INSERT INTO #{test_table_name} VALUES ('correct')")
        end
        expect(
          pg_helper.get_column(
            "SELECT test_text FROM #{test_table_name} order by test_text"
          )
        ).to eq %w(correct pass)
      end
    end

    it 'will not allow nested transaction' do
      expect do
        pg_helper.transaction do |trans|
          trans.transaction { nil }
        end
      end.to raise_error(PgHelperErrorNestedTransactionNotAllowed)
    end
  end
end
