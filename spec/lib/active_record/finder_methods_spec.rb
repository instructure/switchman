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
    end
  end
end
