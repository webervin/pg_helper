require File.expand_path('../../spec_helper', __FILE__)
require 'pg_helper/connection_pool'

describe PgHelper::ConnectionPool do
  describe '.new' # OPTIONS: :pool_size, :checkout_timeout, REST is passed directly to pg_connection.new
  describe '#checkout'
  describe '#checkin(connection)'
  describe '#'

  def connection_options
    {:dbname => 'postgres', :user => 'postgres', :host => 'localhost'}
  end

  subject{described_class.new(connection_options)}

  describe '#auto_connect' do
    it 'is true by default' do
      subject.auto_connect.should be_true
    end

    context 'when is true' do
      before{ subject.auto_connect = true}

      it 'provides active connection' do
         subject.connection.should_not be_finished
       end

       it 'yields active connection' do
         subject.with_connection{|c| c.should_not be_finished}
       end
    end

    context 'when is false' do
      before{ subject.auto_connect = false}
      it 'raises error instead of active connection' do
        expect {
          subject.connection
        }.to raise_exception
      end

      it 'raises error instead of yielding active connection' do
        expect {
          subject.connection
        }.to raise_exception
      end
    end

  end

end