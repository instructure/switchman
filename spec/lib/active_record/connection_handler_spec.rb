require "spec_helper"

module Switchman
  module ActiveRecord
    describe ConnectionHandler do
      include RSpecHelper

      it "should use different proxies for different categories" do
        Shard.connection_pool.should_not == User.connection_pool
      end

      it "should share underlying pools for different categories on the same shard" do
        Shard.connection_pool.current_pool.should == User.connection_pool.current_pool
      end

      it "should insert sharding for connections established after initialization" do
        User.connection_pool.should == ::ActiveRecord::Base.connection_pool
        begin
          config = { :adapter => 'sqlite3', :database => ':memory:', :something_unique_in_the_spec => true }
          User.establish_connection(config)
          User.connection_pool.should_not == ::ActiveRecord::Base.connection_pool
          User.connection_pool.spec.config.should == config
          User.connection_pool.should be_is_a(ConnectionPoolProxy)
          @shard2.activate do
            User.connection_pool.spec.config.should == ::ActiveRecord::Base.connection_pool.spec.config
            User.connection_pool.spec.config.should_not == config
          end
        ensure
          User.remove_connection
          User.connection_pool.should == ::ActiveRecord::Base.connection_pool
          User.connection_pool.should be_is_a(ConnectionPoolProxy)
        end
      end

      it "should set up separate pools for different categories" do
        User.connection_pool.should_not == MirrorUser.connection_pool
        mu = MirrorUser.create!
        MirrorUser.find(mu.local_id).should == mu
        # didn't activate the :mirror_universe category
        @shard1.activate { MirrorUser.find(mu.local_id).should == mu }
        @shard1.activate(:mirror_universe) { MirrorUser.find_by_id(mu.local_id).should == nil }
      end
    end
  end
end
