require File.expand_path('../../spec_helper', __FILE__)
require 'pg_helper/query_builder.rb'

include PgHelper
describe QueryBuilder do
  subject {QueryBuilder.new('table_name')}

  describe '#to_sql' do

    it 'builds default' do
      subject.to_sql.should == 'SELECT * FROM table_name'
    end

    describe '#select'  do
      it 'for specific column' do
        subject.select('a').to_sql.should == 'SELECT a FROM table_name'
      end

      it 'for multiple columns' do
        subject.select('a as foo').select('b as bar, c').to_sql.should ==
          'SELECT a as foo,b as bar, c FROM table_name'
      end
    end

    describe '#where' do
      it 'using single condition' do
        subject.where('a = b').to_sql.should == 'SELECT * FROM table_name WHERE a = b'
      end

      it 'ands multiple conditions' do
        subject.where('a = 1').where('b = 2').to_sql.should == 'SELECT * FROM table_name WHERE a = 1 AND b = 2'
      end
    end

    describe '#with' do
      it 'single CTE' do
        subject.with('foo', 'select bar from foo').to_sql.should ==
          'WITH foo AS (select bar from foo) SELECT * FROM table_name'
      end

      it 'multiple CTEs' do
        subject.with('foo', 'select 1').with('bar', 'select a').to_sql.should ==
          'WITH foo AS (select 1),bar AS (select a) SELECT * FROM table_name'
      end
    end

    describe '#join' do
      it 'can join single table' do
        subject.join('LEFT JOIN bar on bar.id = table_name.id').to_sql.should ==
          'SELECT * FROM table_name LEFT JOIN bar on bar.id = table_name.id'
      end

      it 'can do multiple joins' do
        subject.join('CROSS JOIN bar').join('INNER JOIN foo using (buz)').to_sql.should ==
          'SELECT * FROM table_name CROSS JOIN bar INNER JOIN foo using (buz)'
      end
    end
  end
end