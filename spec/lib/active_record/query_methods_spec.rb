require "spec_helper"

module Switchman
  module ActiveRecord
    describe QueryMethods do
      include RSpecHelper

      before do
        @user1 = User.create!
        @user2 = @shard1.activate { User.create! }
        @user3 = @shard2.activate { User.create! }
      end

      describe "#primary_shard" do
        it "should be the shard if it's a shard" do
          User.shard(Shard.default).primary_shard.should == Shard.default
          User.shard(@shard1).primary_shard.should == @shard1
        end

        it "should be the first shard of an array of shards" do
          User.shard([Shard.default, @shard1]).primary_shard.should == Shard.default
          User.shard([@shard1, Shard.default]).primary_shard.should == @shard1
        end

        it "should be the object's shard if it's a model" do
          User.shard(@user1).primary_shard.should == Shard.default
          User.shard(@user2).primary_shard.should == @shard1
        end

        it "should be the default shard if it's a scope of Shard" do
          User.shard(Shard.scoped).primary_shard.should == Shard.default
          @shard1.activate do
            User.shard(Shard.scoped).primary_shard.should == Shard.default
          end
        end
      end

      describe "#shard" do
        it "should default to the current shard" do
          relation = User.scoped
          relation.shard_value.should == Shard.default
          relation.shard_source_value.should == :implicit

          @shard1.activate do
            relation.shard_value.should == Shard.default

            relation = User.scoped
            relation.shard_value.should == @shard1
            relation.shard_source_value.should == :implicit
          end
          relation.shard_value.should == @shard1
        end

        it "should be changeable, and change conditions when it is changed" do
          relation = User.where(:id => @user1).shard(@shard1)
          relation.shard_value.should == @shard1
          relation.shard_source_value.should == :explicit
          relation.where_values.first.right.should == @user1.global_id
        end

        it "should infer the shard from a single argument" do
          relation = User.where(:id => @user2)
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == @shard1
          relation.where_values.first.right.should == @user2.local_id
        end

        it "should infer the shard from multiple arguments" do
          relation = User.where(:id => [@user2, @user2])
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == @shard1
          relation.where_values.first.right.should == [@user2.local_id, @user2.local_id]
        end

        it "should infer the correct shard from an array of 1" do
          relation = User.where(:id => [@user2])
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == @shard1
          relation.where_values.first.right.should == [@user2.local_id]
        end

        it "should do nothing when it's an array of 0" do
          relation = User.where(:id => [])
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == Shard.default
          relation.where_values.first.right.should == []
        end

        it "should order the shards preferring the shard it already had as primary" do
          relation = User.where(:id => [@user1, @user2])
          relation.shard_value.should == [Shard.default, @shard1]
          relation.where_values.first.right.should == [@user1.local_id, @user2.global_id]

          @shard1.activate do
            relation = User.where(:id => [@user1, @user2])
            relation.shard_value.should == [@shard1, Shard.default]
            relation.where_values.first.right.should == [@user1.global_id, @user2.local_id]
          end
        end
      end
    end
  end
end
