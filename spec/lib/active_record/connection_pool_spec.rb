require "spec_helper"

module Switchman
  module ActiveRecord
    describe ConnectionPool do
      it "should be able to access another shard on a db server after the 'primary' shard is gone" do
        skip 'A "real" database"' unless Shard.default.database_server.shareable?
        # separate connections
        server = DatabaseServer.create(:config => Shard.default.database_server.config.dup)
        s1 = server.shards.create!(:name => 'non_existent_shard') # don't actually create any schema
        s2 = server.shards.create! # inherit's the default shard's config, which is functional
        s1.activate do
          expect { User.count }.to raise_exception
        end
        # the config for s1 should not be the permanent default for all new
        # connections now
        s2.activate do
          expect { User.count }.not_to raise_exception
        end
      end

      describe "clear_idle_connections!" do
        before do
          skip 'A "real" database"' unless Shard.default.database_server.shareable?
          @server = DatabaseServer.create(:config => Shard.default.database_server.config.dup)
          @shard = @server.shards.create!
          @conn, @pool = @shard.activate{ [User.connection, User.connection_pool.current_pool] }
        end

        it "should disconnect idle connections" do
          @pool.checkin(@conn)
          @conn.expects(:disconnect!).once
          @pool.clear_idle_connections!(@conn.last_query_at + 1)
        end

        it "should remove idle connections" do
          @pool.checkin(@conn)
          @pool.clear_idle_connections!(@conn.last_query_at + 1)
          expect(@pool.connections).to be_empty
        end

        it "should not affect idle but checked out connections" do
          @conn.expects(:disconnect!).never
          @pool.clear_idle_connections!(@conn.last_query_at + 1)
        end

        it "should not affect checked in but recently active connections" do
          @pool.checkin(@conn)
          @conn.expects(:disconnect!).never
          @pool.clear_idle_connections!(@conn.last_query_at - 1)
        end
      end

      describe "release_connection" do
        before do
          skip 'A "real" database"' unless Shard.default.database_server.shareable?
          @server = DatabaseServer.create(:config => Shard.default.database_server.config.dup)
          @shard = @server.shards.create!
          @pool = @shard.activate{ User.connection_pool.current_pool }
          @timeout_was = @pool.spec.config[:idle_timeout]
        end

        after do
          @pool.spec.config[:idle_timeout] = @timeout_was
        end

        it "should clear idle connections if idle timeout is configured" do
          @pool.spec.config[:idle_timeout] = 1.minute
          @pool.expects(:clear_idle_connections!).at_least_once
          @pool.release_connection
        end

        it "should still work if idle timeout is not configured" do
          @pool.spec.config[:idle_timeout] = nil
          @pool.expects(:clear_idle_connections!).never
          expect { @pool.release_connection }.not_to raise_exception
        end
      end
    end
  end
end
