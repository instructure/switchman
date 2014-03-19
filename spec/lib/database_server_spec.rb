require "spec_helper"

module Switchman
  describe DatabaseServer do
    describe "shareable?" do
      it "should be false for sqlite" do
        db = DatabaseServer.new(config: { adapter: 'sqlite3', database: '%{shard_name}' })
        db.shareable?.should be_false
      end

      it "should be true for mysql" do
        db = DatabaseServer.new(config: { adapter: 'mysql' })
        db.shareable?.should be_true

        db = DatabaseServer.new(config: { adapter: 'mysql2' })
        db.shareable?.should be_true
      end

      it "should be true for postgres with a non-variable username" do
        db = DatabaseServer.new(config: { adapter: 'postgresql' })
        db.shareable?.should be_true
      end

      it "should be false for postgres with variable username" do
        db = DatabaseServer.new(config: { adapter: 'postgresql', username: '%{schema_search_path}' })
        db.shareable?.should be_false
      end

      it "should depend on the database environment" do
        db = DatabaseServer.new(config: { adapter: 'postgresql', username: '%{schema_search_path}', deploy: { username: 'deploy' }})
        db.shareable?.should be_false
        ::Shackles.activate(:deploy) { db.shareable? }.should be_true
      end
    end

    describe "#create_new_shard" do
      include RSpecHelper

      def maybe_activate(shard)
        shard.activate { yield } if shard
        yield unless shard
      end

      adapter = ::ActiveRecord::Base.connection.adapter_name
      def create_shard(server)
        new_shard = server.create_new_shard
        new_shard.should_not be_new_record
        new_shard.name.should match /shard_#{new_shard.id}/
        # They should share a connection pool
        if server == Shard.default.database_server
          User.connection_pool.current_pool.should == new_shard.activate { User.connection_pool.current_pool }
          User.connection_pool.current_pool.should == Shard.connection_pool.current_pool
        else
          User.connection_pool.current_pool.should_not == new_shard.activate { User.connection_pool.current_pool }
        end
        # The tables should be created, ready to use
        new_shard.activate {
          a = User.create!
          a.should_not be_new_record
        }
      ensure
        if new_shard
          new_shard.drop_database
          new_shard.destroy
        end
      end

      it "should be able to create a new sqlite shard from a given server" do
        @db = DatabaseServer.create(:config => { :adapter => 'sqlite3', :database => '%{shard_name}', :shard_name => ':memory:' })
        create_shard(@db)
      end

      context "non-transactional" do
        self.use_transactional_fixtures = ::ActiveRecord::Base.connection.supports_ddl_transactions?

        it "should be able to create a new shard from the default db" do
          create_shard(Shard.default.database_server)
        end
      end

      it "should be able to create a new shard from a db server that doesn't have any shards" do
        # otherwise it's a repeat of the sqlite spec above
        pending 'A "real" database"' unless %w{MySQL Mysql2 PostgreSQL}.include?(adapter)

        # So, it's really the same server, but we want separate connections
        server = DatabaseServer.create(:config => Shard.default.database_server.config)
        create_shard(server)
      end

      class MyException < Exception; end
      it "should use the connection's db name as temp db name" do
        db = DatabaseServer.new(config: { adapter: 'postgresql' })
        Shard.expects(:create!).with(:name => Shard.default.name, :database_server => db).raises(MyException.new)
        lambda { db.create_new_shard }.should raise_error(MyException)
      end
    end

    describe "#config" do
      it "should return subenvs" do
        base_config = { database: 'db',
                        slave: [nil, { database: 'slave' }],
                        deploy: { username: 'deploy' }}
        ds = DatabaseServer.new(config: base_config)
        ds.config.should == base_config
        ds.config(:slave).should == [{ database: 'db', deploy: base_config[:deploy] },
                                     { database: 'slave', deploy: base_config[:deploy] }]
        ds.config(:deploy).should == { database: 'db', username: 'deploy', slave: base_config[:slave], deploy: base_config[:deploy] }
      end
    end

    describe "#shackles_environment" do
      it "should inherit from Shackles.environment" do
        ds = DatabaseServer.new
        ds.shackles_environment.should == :master
        ::Shackles.activate(:slave) do
          ds.shackles_environment.should == :slave
        end
      end

      it "should override Shackles.environment when explicitly set" do
        ds = DatabaseServer.new
        ds.shackle!
        ds.shackles_environment.should == :slave
        ds.unshackle do
          ds.shackles_environment.should == :master
        end
        ds.shackles_environment.should == :slave
        ::Shackles.activate(:slave) do
          ds.shackles_environment.should == :slave
          ds.unshackle do
            ds.shackles_environment.should == :slave
          end
          ds.shackles_environment.should == :slave
        end
        ds.shackles_environment.should == :slave
      end
    end

    describe "#cache_store" do
      before do
        @db = DatabaseServer.new
        @default_store = ::ActiveSupport::Cache.lookup_store(:null_store)
        @db_store = ::ActiveSupport::Cache.lookup_store(:memory_store)
        @original_map = Switchman.config[:cache_map]
        Switchman.config[:cache_map] = { ::Rails.env => @default_store, @db.id => @db_store }
      end

      after do
        Switchman.config[:cache_map] = @original_map
      end

      it "should prefer the cache specific to the database" do
        @db.cache_store.object_id.should == @db_store.object_id
      end

      it "should fallback to Rails.cache_without_sharding if no specific cache" do
        Switchman.config[:cache_map].delete(@db.id)
        @db.cache_store.object_id.should == @default_store.object_id
      end
    end

    describe ".server_for_new_shard" do
      before(:all) do
        @db1 = DatabaseServer.find(nil)
        @old_open = @db1.config.delete(:open)
        @old_servers = DatabaseServer.all
        @old_servers.delete(@db1)
        @old_servers.each do |db|
          db.destroy unless db == @db1
        end
      end

      before do
        @db1.config.delete(:open)
      end

      after do
        @db2.try(:destroy)
      end

      after(:all) do
        @db1.config[:open] = @old_open
        @old_servers.each do |db|
          DatabaseServer.create(:id => db.id, :config => db.config)
        end
      end

      it "should return the default server if that's the only one around" do
        DatabaseServer.server_for_new_shard.should == @db1
      end

      it "should return on open server" do
        @db1.config[:open] = true
        DatabaseServer.server_for_new_shard.should == @db1
      end

      it "should return another server if it's the only one open" do
        @db2 = DatabaseServer.create(:config => { :open => true})
        4.times { DatabaseServer.server_for_new_shard.should == @db2 }
        @db2.config.delete(:open)
        @db1.config[:open] = true
        4.times { DatabaseServer.server_for_new_shard.should == @db1 }
      end

      it "should return multiple open servers" do
        @db2 = DatabaseServer.create(:config => { :open => true })
        @db1.config[:open] = true
        dbs = []
        20.times do
          dbs << DatabaseServer.server_for_new_shard
        end
        dbs.should include(@db1)
        dbs.should include(@db2)
      end
    end
  end
end
