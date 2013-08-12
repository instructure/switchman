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

        it "should return foreign keys relative to the current shard" do
          appendage = Appendage.create!

          # bypass the setter; we're going to test it in just a minute

          # local id, should stay local
          appendage.original_user_id = 6
          appendage.user_id.should == 6

          # (incorrect) self referencing global id; should come out as local
          appendage.original_user_id = Shard.current.global_id_for(6)
          appendage.user_id.should == 6

          # global id referencing another shard; should come out unscathed
          appendage.original_user_id = @shard1.global_id_for(6)
          appendage.user_id.should == @shard1.global_id_for(6)

          @shard1.activate do
            # local id in another shard, should be global in this shard
            appendage.original_user_id = 6
            appendage.user_id.should == Shard.default.global_id_for(6)

            # (incorrect) self referencing global id; should come out as global in this shard
            appendage.original_user_id = Shard.default.global_id_for(6)
            appendage.user_id.should == Shard.default.global_id_for(6)

            # global id referencing this shard; should come out as a local id in this shard
            appendage.original_user_id = @shard1.global_id_for(6)
            appendage.user_id.should == 6

            # global id from an unrelated shard; should stay global
            appendage.original_user_id = @shard2.global_id_for(6)
            appendage.user_id.should == @shard2.global_id_for(6)
          end

          # now that we trust the getters, try the setters

          # local stays local
          appendage.user_id = 6
          appendage.original_user_id.should == 6
          appendage.user_id = '6'
          appendage.original_user_id.should == 6

          # (incorrect) global id to this shard, should become local
          appendage.user_id = Shard.current.global_id_for(6)
          appendage.original_user_id.should == 6
          appendage.user_id = Shard.current.global_id_for(6).to_s
          appendage.original_user_id.should == 6

          # global id from another shard, should stay global
          appendage.user_id = @shard1.global_id_for(6)
          appendage.original_user_id.should == @shard1.global_id_for(6)
          appendage.local_user_id.should == 6
          appendage.user_id = @shard1.global_id_for(6).to_s
          appendage.original_user_id.should == @shard1.global_id_for(6)
          appendage.local_user_id.should == 6

          @shard1.activate do
            # local to this shard becomes global
            appendage.user_id = 6
            appendage.original_user_id.should == @shard1.global_id_for(6)
            appendage.user_id = '6'
            appendage.original_user_id.should == @shard1.global_id_for(6)

            # global id from original shard, should become local
            appendage.user_id = Shard.default.global_id_for(6)
            appendage.original_user_id.should == 6
            appendage.user_id = Shard.default.global_id_for(6).to_s
            appendage.original_user_id.should == 6

            # global id from this shard, should stay global
            appendage.user_id = Shard.current.global_id_for(6)
            appendage.original_user_id.should == @shard1.global_id_for(6)
            appendage.user_id = Shard.current.global_id_for(6).to_s
            appendage.original_user_id.should == @shard1.global_id_for(6)

            # global id from unrelated shard, should stay global
            appendage.user_id = @shard2.global_id_for(6)
            appendage.original_user_id.should == @shard2.global_id_for(6)
            appendage.user_id = @shard2.global_id_for(6).to_s
            appendage.original_user_id.should == @shard2.global_id_for(6)
          end
        end
      end
    end
  end
end
