# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe Migrator do
      describe "#with_advisory_lock_connection" do
        include RSpecHelper
        if ::Rails.version < "7.1"
          let(:migrator) do
            ::ActiveRecord::Migrator.new(:up,
                                         [],
                                         ::ActiveRecord::SchemaMigration)
          end
        else
          let(:migrator) do
            ::ActiveRecord::Migrator.new(:up,
                                         [],
                                         ::ActiveRecord::SchemaMigration.new(::ActiveRecord::Base.connection),
                                         ::ActiveRecord::InternalMetadata.new(::ActiveRecord::Base.connection))
          end
        end

        it "hits the same db as the shard we're using" do
          @shard2.activate do
            migrator.with_advisory_lock_connection do |conn|
              expect(conn.pool.db_config.configuration_hash[:server2]).to be true
            end
          end
        end

        it "strips out prefer_secondary from db config" do
          config = ::ActiveRecord::Base.connection_db_config.configuration_hash.dup
          config[:prefer_secondary] = true
          expect(::ActiveRecord::Base.connection_db_config).to receive(:configuration_hash).and_return(config)

          migrator.with_advisory_lock_connection do |conn|
            expect(conn.pool.db_config.configuration_hash[:prefer_secondary]).to be_nil
          end
        end
      end
    end
  end
end
