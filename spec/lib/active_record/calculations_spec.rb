require "spec_helper"

module Switchman
  module ActiveRecord
    describe Calculations do
      include RSpecHelper

      describe "#pluck" do
        before do
          @shard1.activate do
            @user1 = User.create!(:name => "user1")
            @appendage1 = @user1.appendages.create!
          end
          @shard2.activate do
            @user2 = User.create!(:name => "user2")
            @appendage2 = @user2.appendages.create!
          end
        end

        it "should return non-id columns" do
          User.where(:id => [@user1.id, @user2.id]).pluck(:name).sort.should == ["user1", "user2"]
        end

        it "should return primary ids relative to current shard" do
          Appendage.where(:id => @appendage1).pluck(:id).should == [@appendage1.global_id]
          Appendage.where(:id => @appendage2).pluck(:id).should == [@appendage2.global_id]
          @shard1.activate do
            Appendage.where(:id => @appendage1).pluck(:id).should == [@appendage1.local_id]
            Appendage.where(:id => @appendage2).pluck(:id).should == [@appendage2.global_id]
          end
          @shard2.activate do
            Appendage.where(:id => @appendage1).pluck(:id).should == [@appendage1.global_id]
            Appendage.where(:id => @appendage2).pluck(:id).should == [@appendage2.local_id]
          end
        end

        it "should return foreign ids relative to current shard" do
          Appendage.where(:id => @appendage1).pluck(:user_id).should == [@user1.global_id]
          Appendage.where(:id => @appendage2).pluck(:user_id).should == [@user2.global_id]
          @shard1.activate do
            Appendage.where(:id => @appendage1).pluck(:user_id).should == [@user1.local_id]
            Appendage.where(:id => @appendage2).pluck(:user_id).should == [@user2.global_id]
          end
          @shard2.activate do
            Appendage.where(:id => @appendage1).pluck(:user_id).should == [@user1.global_id]
            Appendage.where(:id => @appendage2).pluck(:user_id).should == [@user2.local_id]
          end
        end
      end
    end
  end
end
