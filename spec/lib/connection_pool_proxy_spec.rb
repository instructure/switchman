require "spec_helper"

module Switchman
  describe ConnectionPoolProxy do
    include RSpecHelper

    it "should not share connections for sqlite shards on the same db" do
      @db = DatabaseServer.create(adapter: 'sqlite3', database: ':memory:')
      @sqlite_shard1 = @db.shards.create!
      @sqlite_shard2 = @db.shards.create!
      expect(::ActiveRecord::Base.connection).not_to eq @sqlite_shard2.activate { ::ActiveRecord::Base.connection }
      expect(@sqlite_shard1.activate { ::ActiveRecord::Base.connection }).not_to eq @sqlite_shard2.activate { ::ActiveRecord::Base.connection }
    end

    it "should forward clear_idle_connections! to each of its pools" do
      proxy = User.connection_pool
      @shard1.activate{ proxy.current_pool.expects(:clear_idle_connections!).once }
      @shard2.activate{ proxy.current_pool.expects(:clear_idle_connections!).once }
      proxy.clear_idle_connections!(Time.now)
    end

    it "should handle an array of slaves when creating a pool" do
      spec = Object.new
      spec.instance_variable_set(:@config, adapter: Shard.connection_pool.spec.config[:adapter], database: 'master', slave: [ { database: 'slave1' }, { database: 'slave2' }])
      default_pool = stub(spec: spec)
      cache = {}
      proxy = ConnectionPoolProxy.new(:unsharded, default_pool, cache)
      proxy.stubs(:active_shackles_environment).returns(:slave)
      new_pool = proxy.send(:create_pool)
      expect(new_pool.spec.config[:database]).to eq 'slave1'
    end

    it "should share schema caches between connections" do
      conn1 = User.connection
      conn2 = @shard2.activate { User.connection }
      expect(conn1).to_not be conn2
      expect(conn1.schema_cache).to be_a(Switchman::SchemaCache)
      expect(conn1.schema_cache).to be conn2.schema_cache
      expect(conn1.schema_cache.connection).to be conn1
      @shard2.activate do
        expect(conn1.schema_cache.connection).to be conn2
      end
    end

    context "non-transactional" do
      self.use_transactional_tests = false

      describe "#release_connection" do
        it "applies to the default pool too" do
          User.connection
          expect(User.connection_pool.active_connection?).to be_truthy
          User.connection_pool.release_connection
          expect(User.connection_pool.active_connection?).to be_falsey
        end
      end
    end
  end
end
