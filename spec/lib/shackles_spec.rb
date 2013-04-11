require "spec_helper"

module Switchman
  describe Shackles do
    include RSpecHelper

    before do
      #!!! trick Shackles in to actually switching envs
      Rails.env.stubs(:test?).returns(false)

      # be sure to test bugs where the current env isn't yet included in this hash
      ::Shackles.connection_handlers.clear
    end

    it "should call ensure_handler when switching envs" do
      old_handler = ::ActiveRecord::Base.connection_handler
      ::Shackles.expects(:ensure_handler).returns(old_handler).twice
      ::Shackles.activate(:slave) {}
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
      @default_pools = ::ActiveRecord::Base.connection_handler.connection_pools
      ::Shackles.activate(:slave_that_no_one_else_uses) do
        pools = ::ActiveRecord::Base.connection_handler.connection_pools
        @default_pools.keys.sort.should == pools.keys.sort
        @default_pools.keys.each do |model|
          @default_pools[model].should_not == pools[model]
        end
        # should have the same number of distinct default_pools
        pools.values.map(&:default_pool).uniq.length.should == @default_pools.values.map(&:default_pool).uniq.length
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
