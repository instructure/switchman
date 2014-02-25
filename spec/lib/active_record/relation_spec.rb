require "spec_helper"

module Switchman
  module ActiveRecord
    describe Relation do
      include RSpecHelper

      before do
        @user1 = User.create!(:name => 'user1')
        @user2 = @shard1.activate { User.create!(:name => 'user2') }
      end

      describe "#exec_queries" do
        it "should activate the correct shard for the query" do
          User.shard(@shard1).where(:id => @user2.local_id).first.should == @user2
        end

        it "should activate multiple shards if necessary" do
          User.where(:id => [@user1.id, @user2.id]).all.sort_by(&:id).should == [@user1, @user2].sort_by(&:id)
        end
      end

      describe "#update_all" do
        it "should activate the correct shard for the query" do
          User.shard(@shard1).where(:id => @user2.local_id).update_all(:name => 'a')
          @user1.reload.name.should == 'user1'
          @user2.reload.name.should == 'a'
        end

        it "should activate multiple shards if necessary" do
          User.where(:id => [@user1.id, @user2.id]).update_all(:name => 'a')
          @user1.reload.name.should == 'a'
          @user2.reload.name.should == 'a'
        end
      end

      describe "#new" do
        it "should infer the scope's shard" do
          scope = @shard1.activate { User.where(id: 1) }
          u = scope.new
          u.shard.should == @shard1
          u.local_id.should == 1
        end
      end
    end
  end
end
