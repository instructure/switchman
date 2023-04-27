# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe ConnectionPool do
      context "with our protections" do
        self.use_transactional_tests = false
        include RSpecHelper

        it "is able to access another shard on a db server after the 'primary' shard is gone" do
          # separate connections
          server = DatabaseServer.create(Shard.default.database_server.config)
          s1 = server.shards.create!(name: "non_existent_shard") # don't actually create any schema
          s2 = server.shards.create! # inherit's the default shard's config, which is functional
          s1.activate do
            expect { User.count }.to raise_error(::ActiveRecord::StatementInvalid)
          end
          # the config for s1 should not be the permanent default for all new
          # connections now
          s2.activate do
            expect { User.count }.not_to raise_error
          end
        end
      end

      it "raises an error when a non-existent shard is activated" do
        Shard.new.activate do
          expect { User.count }.to raise_error(Errors::NonExistentShardError)
        end
      end

      describe "*_schema_cache" do
        before do
          @server = DatabaseServer.create(Shard.default.database_server.config)
          @shard = @server.shards.create!
          @p1 = @shard.activate do
            User.connection_pool.get_schema_cache(User.connection)
            User.connection_pool
          end

          User.connection_pool.get_schema_cache(User.connection)
          @p2 = User.connection_pool
        end

        it "shares the same schema cache across all connection pools" do
          expect(@p1).not_to be(@p2)
          expect(@p1.schema_cache).to be(@p2.schema_cache)
        end

        it "replaces the shared schema cache with the new version" do
          connection = ::ActiveRecord::Base.connection
          new_schema_cache = ::ActiveRecord::ConnectionAdapters::SchemaCache.new(connection)
          new_schema_cache.connection = connection

          expect(new_schema_cache.size).not_to eq(@p1.schema_cache.size) # sanity check

          @p1.set_schema_cache(new_schema_cache)

          expect(@p1.schema_cache).to be(@p2.schema_cache)
          expect(@p1.schema_cache.size).to eq(@p2.schema_cache.size)
        end

        it "uses the shared schema cache if not already set" do
          p3 = DatabaseServer.create(Shard.default.database_server.config).shards.create!.activate do
            User.connection_pool
          end

          connection = ::ActiveRecord::Base.connection
          new_schema_cache = ::ActiveRecord::ConnectionAdapters::SchemaCache.new(connection)
          new_schema_cache.connection = connection
          new_schema_cache.columns("users")

          # sanity check
          expect(p3.schema_cache).to be_nil
          expect(new_schema_cache.size).not_to eq(@p2.schema_cache)

          @p1.set_schema_cache(new_schema_cache)

          expect(@p1.schema_cache).to be(@p2.schema_cache)
          expect(@p1.schema_cache.size).to eq(new_schema_cache.size)
          expect(@p2.schema_cache.size).to eq(new_schema_cache.size)
        end
      end

      describe "release_connection" do
        before do
          @server = DatabaseServer.create(Shard.default.database_server.config)
          @shard = @server.shards.create!
          @pool = @shard.activate { User.connection_pool }
        end

        it "calls flush when releasing connection" do
          expect(@pool).to receive(:flush)
          @pool.release_connection
        end
      end
    end
  end
end
