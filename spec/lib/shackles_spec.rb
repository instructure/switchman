require "spec_helper"

module Switchman
  describe Shackles do
    include RSpecHelper

    before do
      #!!! trick Shackles in to actually switching envs
      ::Rails.env.stubs(:test?).returns(false)

      # be sure to test bugs where the current env isn't yet included in this hash
      ::Shackles.connection_handlers.clear
    end

    it "should capture the correct current_pool" do
      # use @shard2 cause it has its own DatabaseServer
      @shard2.activate do
        @current_pool = ::Shackles.activate(:slave) { ::ActiveRecord::Base.connection_pool.current_pool }
      end
      ::Shackles.activate(:slave) do
        ::ActiveRecord::Base.connection_pool.default_pool.should_not == @current_pool
      end
    end

    it "should correctly set up pools for sharding categories" do
      models = ::ActiveRecord::Base.connection_handler.connection_pools
      default_pools = Hash[models.map { |k, v| [k, v.current_pool] }]
      ::Shackles.activate(:slave_that_no_one_else_uses) do
        models = ::ActiveRecord::Base.connection_handler.connection_pools
        pools = Hash[models.map { |k, v| [k, v.current_pool] }]
        default_pools.keys.sort.should == pools.keys.sort
        default_pools.keys.each do |model|
          default_pools[model].should_not == pools[model]
        end
      end
    end

    it "should connect to the first working slave" do
      # have to unstub long enough to create this
      ::Rails.env.unstub(:test?)
      ds = DatabaseServer.create(config: Shard.default.database_server.config.merge(
        :slave => [{ host: 'some.postgres.server' }, nil]))
      ::Rails.env.stubs(:test?).returns(false)
      ds.shackle!
      s = ds.shards.create!
      s.activate do
        User.connection
        User.connection_pool.spec.config[:host].should_not == 'some.postgres.server'
      end
    end

    it "should deshackle the appropriate connection when the scope changes connections" do
      begin
        Shard.default.database_server.shackle!
        @shard2.activate do
          Shard.default.database_server.shackles_environment.should == :slave
          ::Shackles.environment.should == :master

          Shard.default.database_server.expects(:unshackle).once
          User.shard(Shard.default).update_all(updated_at: nil)
        end
      ensure
        Shard.default.database_server.unshackle!
      end
    end

    it "should deshackle for FOR UPDATE queries" do
      begin
        Shard.default.database_server.shackle!
        Shard.default.database_server.shackles_environment.should == :slave

        u = User.create!
        Shard.default.database_server.expects(:unshackle).once.returns([])
        User.scoped(lock: true).first
        Shard.default.database_server.expects(:unshackle).once.returns([])
        lambda { u.lock! }.should raise_error(::ActiveRecord::RecordNotFound)
      ensure
        Shard.default.database_server.unshackle!
      end
    end

    it "should not get confused about a single shackled server" do
      begin
        Shard.default.database_server.shackle!
        # have to unstub long enough to create this
        ::Rails.env.unstub(:test?)
        ds = DatabaseServer.create(config: { adapter: 'postgresql', host: 'notshackled', slave: { host: 'shackled' }})
        ::Rails.env.stubs(:test?).returns(false)
        s = ds.shards.create!
        s.activate do
          User.connection_pool.spec.config[:host].should == 'notshackled'
        end
      ensure
        Shard.default.database_server.unshackle!
      end
    end

    context "non-transactional" do
      self.use_transactional_fixtures = false

      it "should really disconnect all envs" do
        ::ActiveRecord::Base.connection
        ::ActiveRecord::Base.connection_pool.should be_connected
        @shard1.activate do
          ::ActiveRecord::Base.connection
          ::ActiveRecord::Base.connection_pool.should be_connected
        end
        @shard2.activate do
          ::ActiveRecord::Base.connection
          ::ActiveRecord::Base.connection_pool.should be_connected
        end

        ::Shackles.activate(:slave) do
          ::ActiveRecord::Base.connection
          ::ActiveRecord::Base.connection_pool.should be_connected
          @shard1.activate do
            ::ActiveRecord::Base.connection
            ::ActiveRecord::Base.connection_pool.should be_connected
          end
          @shard2.activate do
            ::ActiveRecord::Base.connection
            ::ActiveRecord::Base.connection_pool.should be_connected
          end
        end

        ::ActiveRecord::Base.clear_all_connections!
        ::ActiveRecord::Base.connection_pool.should_not be_connected
        @shard1.activate do
          ::ActiveRecord::Base.connection_pool.should_not be_connected
        end
        @shard2.activate do
          ::ActiveRecord::Base.connection_pool.should_not be_connected
        end
        ::Shackles.activate(:slave) do
          ::ActiveRecord::Base.connection_pool.should_not be_connected
          @shard1.activate do
            ::ActiveRecord::Base.connection_pool.should_not be_connected
          end
          @shard2.activate do
            ::ActiveRecord::Base.connection_pool.should_not be_connected
          end
        end
      end

      def actual_connection_count
        ::ActiveRecord::Base.connection_pool.current_pool.instance_variable_get(:@reserved_connections).length
      end

      it "should really return active connections to the pool in all envs" do
        ::ActiveRecord::Base.connection
        actual_connection_count.should_not == 0
        @shard1.activate do
          ::ActiveRecord::Base.connection
          actual_connection_count.should_not == 0
        end
        @shard2.activate do
          ::ActiveRecord::Base.connection
          actual_connection_count.should_not == 0
        end

        ::Shackles.activate(:slave) do
          ::ActiveRecord::Base.connection
          actual_connection_count.should_not == 0
          @shard1.activate do
            ::ActiveRecord::Base.connection
            actual_connection_count.should_not == 0
          end
          @shard2.activate do
            ::ActiveRecord::Base.connection
            actual_connection_count.should_not == 0
          end
        end

        ::ActiveRecord::Base.clear_active_connections!
        actual_connection_count.should == 0
        @shard1.activate do
          actual_connection_count.should == 0
        end
        @shard2.activate do
          actual_connection_count.should == 0
        end
        ::Shackles.activate(:slave) do
          actual_connection_count.should == 0
          @shard1.activate do
            actual_connection_count.should == 0
          end
          @shard2.activate do
            actual_connection_count.should == 0
          end
        end
      end

      it "should not establish connections when switching environments" do
        ::ActiveRecord::Base.clear_all_connections!
        ::ActiveRecord::Base.connection_pool.should_not be_connected
        ::Shackles.activate(:slave) {}
        ::ActiveRecord::Base.connection_pool.should_not be_connected
      end
    end
  end
end
