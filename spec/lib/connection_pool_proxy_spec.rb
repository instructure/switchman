require "spec_helper"

module Switchman
  describe ConnectionPoolProxy do
    include RSpecHelper

    it "should not share connections for sqlite shards on the same db" do
      @db = DatabaseServer.create(:config => { :adapter => 'sqlite3', :database => ':memory:' })
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
  end
end
