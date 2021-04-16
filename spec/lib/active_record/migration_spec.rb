
require "spec_helper"

module Switchman
  module ActiveRecord
    describe Migrator do
      describe "#with_advisory_lock_connection" do
        include RSpecHelper

        it "hits the same db as the shard we're using" do
          skip "only applies to rails 6" unless ::Rails.version >= '6.0'

          @shard2.activate do
            ::ActiveRecord::Migrator.new(:up, [], ::ActiveRecord::SchemaMigration).with_advisory_lock_connection do |conn|
              expect(conn.pool.spec.config[:server2]).to eq true
            end
          end
        end

        it "strips out prefer_secondary from db config" do
          skip "only applies to rails 6" unless ::Rails.version >= '6.0'

          config = ::ActiveRecord::Base.connection_config.dup
          config[:prefer_secondary] = true
          allow(::ActiveRecord::Base).to receive(:connection_config).and_return(config)

          ::ActiveRecord::Migrator.new(:up, [], ::ActiveRecord::SchemaMigration).with_advisory_lock_connection do |conn|
            expect(conn.pool.spec.config[:prefer_secondary]).to be_nil
          end
        end
      end
    end
  end
end
