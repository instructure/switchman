require "spec_helper"

module Switchman
  module ActiveRecord
    describe AttributeMethods do
      include RSpecHelper

      describe "ids" do
        it "should return id relative to the current shard" do
          user = User.create!
          user.id.should < Shard::IDS_PER_SHARD
          user.local_id.should < Shard::IDS_PER_SHARD
          user.global_id.should > Shard::IDS_PER_SHARD

          @shard1.activate do
            user.id.should > Shard::IDS_PER_SHARD
            user.local_id.should < Shard::IDS_PER_SHARD
            user.global_id.should > Shard::IDS_PER_SHARD
          end
        end
      end
    end
  end
end
