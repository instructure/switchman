require "spec_helper"

module Switchman
  describe RSpecHelper do
    context "unsharded" do
      it "doesn't make shards accessible" do
        # by virtue of including RSpecHelper somewhere, test shards will
        # always already be set up by this point (though not accessible),
        # but only if we are running a sharding spec
        Shard.count.should == 0
        Shard.default.should be_a(DefaultShard)
      end

      it "doesn't set up sharding at all if no sharded specs are run" do
        run_groups = RSpec.world.filtered_examples.select{ |k, v| v.present? }.map(&:first)
        pending "run without other sharding specs" if run_groups.any?{ |group| RSpecHelper.included_in?(group) }

        RSpecHelper.class_variable_defined?(:@@default_shard).should be_false
        RSpecHelper.class_variable_get(:@@shard1).should be_nil
      end

      it "sets up sharding but hides it if other sharding specs are run" do
        run_groups = RSpec.world.filtered_examples.select{ |k, v| v.present? }.map(&:first)
        pending "run alongside sharding specs" unless run_groups.any?{ |group| RSpecHelper.included_in?(group) }

        RSpecHelper.class_variable_get(:@@default_shard).should be_a(Shard)
        RSpecHelper.class_variable_get(:@@shard1).should be_a(Shard)
      end
    end

    context "sharding" do
      # strategically place these before we include the module
      before(:all) do
        Shard.default.should be_a(DefaultShard)
      end

      after(:all) do
        Shard.default.should be_a(DefaultShard)
      end

      include RSpecHelper

      it "should make the default shard a real shard" do
        Shard.default.should be_a(Shard)
      end
    end

    context "transactions" do
      include RSpecHelper

      def begin_transaction(conn)
        if ::Rails.version < '4'
          conn.transaction_joinable = false
          conn.begin_db_transaction
          conn.increment_open_transactions
        else
          conn.begin_transaction joinable: false
        end
      end

      def rollback_transaction(conn)
        if ::Rails.version < '4'
          conn.decrement_open_transactions
          if conn.open_transactions == 0
            conn.rollback_db_transaction
          else
            conn.rollback_to_savepoint
          end
        else
          conn.rollback_transaction
        end
      end

      before :all do
        @shard2.activate do
          begin_transaction(::ActiveRecord::Base.connection)
          User.create!
        end
      end

      it "should support nested transactions" do
        @shard2.activate do |shard|
          User.count.should == 1 # we get the user from the before :all
          User.create! # should only last for the duration of this spec
          conn = ::ActiveRecord::Base.connection
          conn.open_transactions.should eql 2
        end
      end

      prepend_after :all do
        @shard2.activate do
          User.count.should == 1
          conn = ::ActiveRecord::Base.connection
          conn.open_transactions.should eql 1 # RSpecHelper shouldn't have rolled back the before :all one above
          rollback_transaction(::ActiveRecord::Base.connection)
        end
      end
    end
  end
end
