require "spec_helper"

module Switchman
  module ActiveRecord
    describe Relation do
      include RSpecHelper

      before do
        @user1 = User.create!
        @user2 = @shard1.activate { User.create! }
      end

      describe "#exec_queries" do
        it "should activate the correct shard for the query" do
          User.shard(@shard1).where(:id => @user2.local_id).first.should == @user2
        end

        it "should activate multiple shards if necessary" do
          User.where(:id => [@user1.id, @user2.id]).all.sort_by(&:id).should == [@user1, @user2].sort_by(&:id)
        end
      end
    end
  end
end
