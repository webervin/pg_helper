require File.expand_path('../../spec_helper', __FILE__)
include PgHelper
describe QueryHelper do
  describe 'connection params' do
    let(:params) {
      {
        :host => 'my host',
        :port => 1234,
        :username => 'my user',
        :password => 'my password',
        :database => 'my base'
      }
    }

    it 'are stored upon initializetion' do
      PGconn.stub!(:open => true)
      QueryHelper.new(params).connection_params.should == params
    end

    it 'are passed to pgconn' do
      mock(:conn).tap do |conn|
        PGconn.should_receive(:open).with(params).and_return(conn)
        QueryHelper.new(params).pg_connection.should == conn
      end
    end
  end

  def build_result(params={})
    mock(:result,
         {
           :nfields => 1,
           :ntuples => 1
         }.merge(params)
      ).as_null_object
  end
  
  def mock_result(params = {}, *args)
    build_result(params).tap do |result_mock|
      if args.empty?
        pg_helper.pg_connection.should_receive(:exec).and_return(result_mock)
      else
        pg_helper.pg_connection.should_receive(:exec).with(*args).and_return(result_mock)
      end
    end
  end

  let(:pg_helper) { QueryHelper.new({:database => 'postgres'})}

  describe 'single value' do
    it 'is returned as string' do
      pg_helper.value('select 1').should == '1'
    end

    it 'raises error if gets non array as params' do
      lambda {
        pg_helper.value('select 1', 'bar')
      }.should raise_error(PgHelperErrorParamsMustBeArrayOfStrings)
    end

    it 'allows to pass params to query' do
      param = ["foo; > 'bar'"]
      pg_helper.value('select $1::text', param).should == "foo; > 'bar'"
    end

    it 'raises error if more than 1 row returned' do
      lambda {
        pg_helper.value('select 1, 2')
      }.should raise_error(PgHelperErrorInvalidColumnCount)
    end

    it 'raises error if more than 1 row returned' do
      lambda {
        pg_helper.value('select 1 union select 2')
      }.should raise_error(PgHelperErrorInvalidRowCount)
    end

    it 'clears pg result on success' do
      mock_result.should_receive(:clear)
      pg_helper.value('foo')
    end

    it 'clears pg result on failure' do
      lambda {
        mock_result({:nfields => 2}).should_receive(:clear)
        pg_helper.value('foo')
      }
    end
  end

  describe 'array of column values' do
    it 'returns values of column as array of stirngs' do
      pg_helper.get_column( 'select 1 union (select 2 union select 3)').should == ['1','2','3']
    end

    it 'raises error if gets non array as params' do
       lambda {
         pg_helper.get_column('select 1', 'bar')
       }.should raise_error(PgHelperErrorParamsMustBeArrayOfStrings)
     end

     it 'allows to pass params to query' do
       param = ['foo', ";'bar'"]
       pg_helper.get_column('select $1::text as str union select $2::text as str order by str', param).should == [";'bar'", 'foo']
     end

    it 'raises error if more than one column returned' do
       lambda {
        pg_helper.get_column('select 1, 2')
      }.should raise_error(PgHelperErrorInvalidColumnCount)
    end

    it 'clears pg result on success' do
      mock_result.should_receive(:clear)
      pg_helper.get_column('foo')
    end

    it 'clears pg result on failure' do
      lambda {
        mock_result({:nfields => 2}).should_receive(:clear)
        pg_helper.get_column('foo')
      }
    end
  end

  describe 'executing operation' do
    it 'returns number of rows changed by query' do
      pg_helper.modify('select 1').should == 0
    end

    it 'uses cmd_tuples of pg_result internally' do
      mock(:value).tap do |value|
        mock_result(:cmd_tuples => value)
        pg_helper.modify('foo').should == value
      end
    end

    it 'raises error if gets non array as params' do
      lambda {
        pg_helper.modify('select 1', 'bar')
      }.should raise_error(PgHelperErrorParamsMustBeArrayOfStrings)
    end

    it 'allows to pass params to query' do
      sql = 'update foo set test_text =  $1::text'
      mock(:value).tap do |result|
        mock_result({:cmd_tuples => result}, sql, ['foo'])
        pg_helper.modify(sql, ["foo"]).should == result
      end
    end


    it 'clears pg result on success' do
      mock_result.should_receive(:clear)
      pg_helper.modify('foo')
    end

    it 'clears pg result on failure' do
      lambda {
        mock_result.tap do |result|
          result.should_receive(:cmd_tuples).and_raise(Exception)
          result.should_receive(:clear)
        end
        pg_helper.modify('foo')
      }
    end
  end

  describe 'transaction' do
    it 'raises error if no block given' do
      lambda { pg_helper.transaction}.should raise_error(ArgumentError)
    end

    it 'allows to rollback' do
      pg_helper.transaction do |t|
        t.rollback!
      end
    end

    it 'does not allow rollback if not in transaction' do
      lambda {pg_helper.rollback!}.should raise_error(PgHelperErrorInvalidOutsideTransaction)
    end


    describe 'using temporary table' do
      let(:test_table_name){'pg_helper_test_'+Time.now.to_i.to_s}

      before(:all) do
        sql = <<SQL
        CREATE TEMP TABLE #{test_table_name}
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
        lambda {
          pg_helper.transaction do |t|
            t.modify( "INSERT INTO #{test_table_name} VALUES ('one'), ('two'), ('three')" )
            t.value("SELECT COUNT(*) FROM #{test_table_name}").should == '3'
				    raise Exception.new('roll it back')
          end
        }.should raise_error('roll it back')
        pg_helper.value("SELECT COUNT(*) FROM #{test_table_name}").should == '0'
      end

      it 'will commit in the end' do
        pg_helper.transaction do |t|
          t.modify("INSERT INTO #{test_table_name} VALUES ('pass')")
          t.modify("INSERT INTO #{test_table_name} VALUES ('correct')")
        end
        pg_helper.get_column("SELECT test_text FROM #{test_table_name} order by test_text").should == ["correct", "pass"]
      end
    end

    it 'will not allow nested transaction' do
      lambda {
        pg_helper.transaction do |trans|
          trans.transaction {nil}
        end
      }.should raise_error(PgHelperErrorNestedTransactionNotAllowed)
    end
  end
end
