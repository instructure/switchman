require "spec_helper"

module Switchman
  module ActiveRecord
    describe Base do
      include RSpecHelper

      describe "to_param" do
        it "should return nil if not persisted" do
          user = User.new
          user.to_param.should be_nil
        end

        it "should return local id if in the current shard" do
          user = User.create!
          user.to_param.should == user.local_id
          @shard1.activate do
            user2 = User.create!
            user2.to_param.should == user2.local_id
          end
        end

        it "should return a short form global id if not in the current shard" do
          user = nil
          @shard1.activate do
            user = User.create!
          end
          @shard2.activate do
            user.to_param.should == "#{@shard1.id}~#{user.local_id}"
          end
        end

        it "should use to_param in url helpers" do
          helpers = Rails.application.routes.url_helpers
          user = nil
          appendage = nil

          @shard1.activate do
            user = User.create!
            appendage = Appendage.create!

            helpers.user_path(user).should == "/users/#{user.local_id}"
            helpers.user_appendages_path(user).should == "/users/#{user.local_id}/appendages"
            helpers.user_appendage_path(user, appendage).should == "/users/#{user.local_id}/appendages/#{appendage.local_id}"
            helpers.user_test1_path(user).should == "/users/#{user.local_id}"
            helpers.user_test2_path(user).should == "/users/#{user.local_id}/test2"
          end

          @shard2.activate do
            user_short_id = "#{@shard1.id}~#{user.local_id}"
            appendage_short_id = "#{@shard1.id}~#{appendage.local_id}"

            helpers.user_path(user).should == "/users/#{user_short_id}"
            helpers.user_appendages_path(user).should == "/users/#{user_short_id}/appendages"
            helpers.user_appendage_path(user, appendage).should == "/users/#{user_short_id}/appendages/#{appendage_short_id}"
            helpers.user_test1_path(user).should == "/users/#{user_short_id}"
            helpers.user_test2_path(user).should == "/users/#{user_short_id}/test2"

            appendage2 = Appendage.create!
            helpers.user_appendage_path(user, appendage2).should == "/users/#{user_short_id}/appendages/#{appendage2.local_id}"
          end
        end
      end
    end
  end
end
