require "spec_helper"

module Switchman
  module ActiveRecord
    describe FinderMethods do
      include RSpecHelper

      before do
        @user = @shard1.activate { User.create! }
      end

      describe "#find_one" do
        it "should find with a global id" do
          User.find(@user.global_id).should == @user
        end
      end

      describe "#find_by_attributes" do
        it "should find with a global id" do
          User.find_by_id(@user.global_id).should == @user
        end

        it "should find with an array of global ids" do
          User.find_by_id([@user.global_id]).should == @user
        end
      end

      describe "#find_or_initialize" do
        it "should initialize with the shard from the scope" do
          @user.destroy
          u = User.shard(@shard1).where(id: @user).first_or_initialize
          u.should be_new_record
          u.shard.should == @shard1
        end
      end
    end
  end
end
