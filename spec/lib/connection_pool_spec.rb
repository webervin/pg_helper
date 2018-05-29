require 'spec_helper'
require 'pg_helper/connection_pool'

RSpec.describe PgHelper::ConnectionPool do
  describe '.new'
  # OPTIONS: :pool_size, :checkout_timeout,
  # # REST is passed directly to pg_connection.new
  describe '#checkout'
  describe '#checkin(connection)'
  describe '#'

  def connection_options
    { dbname: 'postgres', user: 'postgres', host: 'localhost' }
  end

  subject { described_class.new(connection_options) }

  describe '#auto_connect' do
    it 'is true by default' do
      expect(subject.auto_connect).to be true
    end

    context 'when is true' do
      before { subject.auto_connect = true }

      it 'provides active connection' do
        expect(subject.connection).not_to be_finished
      end

      it 'yields active connection' do
        subject.with_connection { |c| expect(c).not_to be_finished }
      end
    end

    context 'when is false' do
      before { subject.auto_connect = false }
      it 'raises error instead of active connection' do
        expect do
          subject.connection
        end.to raise_exception(::PgHelper::ConnectionPool::ConnectionNotEstablished)
      end
    end
  end
end
