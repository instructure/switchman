# frozen_string_literal: true

require 'spec_helper'

module Switchman
  module ActiveRecord
    describe Migrator do
      describe '#with_advisory_lock_connection' do
        include RSpecHelper

        it "hits the same db as the shard we're using" do
          @shard2.activate do
            ::ActiveRecord::Migrator.new(:up, [], ::ActiveRecord::SchemaMigration).with_advisory_lock_connection do |conn|
              expect(conn.pool.db_config.configuration_hash[:server2]).to eq true
            end
          end
        end

        it 'strips out prefer_secondary from db config' do
          config = ::ActiveRecord::Base.connection_db_config.configuration_hash.dup
          config[:prefer_secondary] = true
          allow(::ActiveRecord::Base.connection_db_config).to receive(:configuration_hash).and_return(config)

          ::ActiveRecord::Migrator.new(:up, [], ::ActiveRecord::SchemaMigration).with_advisory_lock_connection do |conn|
            expect(conn.pool.db_config.configuration_hash[:prefer_secondary]).to be_nil
          end
        end
      end
    end
  end
end
