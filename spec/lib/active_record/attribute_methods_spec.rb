require "spec_helper"

module Switchman
  module ActiveRecord
    describe AttributeMethods do
      include RSpecHelper

      describe "ids" do
        it "should return id relative to the current shard" do
          user = User.create!
          expect(user.id).to be < Shard::IDS_PER_SHARD
          expect(user.local_id).to be < Shard::IDS_PER_SHARD
          expect(user.global_id).to be > Shard::IDS_PER_SHARD

          @shard1.activate do
            expect(user.id).to be > Shard::IDS_PER_SHARD
            expect(user.local_id).to be < Shard::IDS_PER_SHARD
            expect(user.global_id).to be > Shard::IDS_PER_SHARD
          end
        end

        it "should return foreign keys relative to the current shard" do
          appendage = Appendage.create!

          # bypass the setter; we're going to test it in just a minute

          # local id, should stay local
          appendage.original_user_id = 6
          expect(appendage.user_id).to eq 6

          # (incorrect) self referencing global id; should come out as local
          appendage.original_user_id = Shard.current.global_id_for(6)
          expect(appendage.user_id).to eq 6

          # global id referencing another shard; should come out unscathed
          appendage.original_user_id = @shard1.global_id_for(6)
          expect(appendage.user_id).to eq @shard1.global_id_for(6)

          @shard1.activate do
            # local id in another shard, should be global in this shard
            appendage.original_user_id = 6
            expect(appendage.user_id).to eq Shard.default.global_id_for(6)

            # (incorrect) self referencing global id; should come out as global in this shard
            appendage.original_user_id = Shard.default.global_id_for(6)
            expect(appendage.user_id).to eq Shard.default.global_id_for(6)

            # global id referencing this shard; should come out as a local id in this shard
            appendage.original_user_id = @shard1.global_id_for(6)
            expect(appendage.user_id).to eq 6

            # global id from an unrelated shard; should stay global
            appendage.original_user_id = @shard2.global_id_for(6)
            expect(appendage.user_id).to eq @shard2.global_id_for(6)
          end

          # now that we trust the getters, try the setters

          # local stays local
          appendage.user_id = 6
          expect(appendage.original_user_id).to eq 6
          appendage.user_id = '6'
          expect(appendage.original_user_id).to eq 6

          # (incorrect) global id to this shard, should become local
          appendage.user_id = Shard.current.global_id_for(6)
          expect(appendage.original_user_id).to eq 6
          appendage.user_id = Shard.current.global_id_for(6).to_s
          expect(appendage.original_user_id).to eq 6

          # global id from another shard, should stay global
          appendage.user_id = @shard1.global_id_for(6)
          expect(appendage.original_user_id).to eq @shard1.global_id_for(6)
          expect(appendage.local_user_id).to eq 6
          appendage.user_id = @shard1.global_id_for(6).to_s
          expect(appendage.original_user_id).to eq @shard1.global_id_for(6)
          expect(appendage.local_user_id).to eq 6

          @shard1.activate do
            # local to this shard becomes global
            appendage.user_id = 6
            expect(appendage.original_user_id).to eq @shard1.global_id_for(6)
            appendage.user_id = '6'
            expect(appendage.original_user_id).to eq @shard1.global_id_for(6)

            # global id from original shard, should become local
            appendage.user_id = Shard.default.global_id_for(6)
            expect(appendage.original_user_id).to eq 6
            appendage.user_id = Shard.default.global_id_for(6).to_s
            expect(appendage.original_user_id).to eq 6

            # global id from this shard, should stay global
            appendage.user_id = Shard.current.global_id_for(6)
            expect(appendage.original_user_id).to eq @shard1.global_id_for(6)
            appendage.user_id = Shard.current.global_id_for(6).to_s
            expect(appendage.original_user_id).to eq @shard1.global_id_for(6)

            # global id from unrelated shard, should stay global
            appendage.user_id = @shard2.global_id_for(6)
            expect(appendage.original_user_id).to eq @shard2.global_id_for(6)
            appendage.user_id = @shard2.global_id_for(6).to_s
            expect(appendage.original_user_id).to eq @shard2.global_id_for(6)
          end
        end

        it "should not choke on polymorphic associations that are missing their type" do
          f = Feature.create!(owner: User.create!)
          f = Feature.select(:owner_id).where(id: f).first
          f.owner_id
        end

        it "gives a useful error if the association doesn't exist" do
          u = User.new
          expect { u.global_broken_id }.to raise_error do |error|
            expect(error).to be_a(NoMethodError)
            expect(error.to_s).to eq "undefined method `global_broken_id'; are you missing an association?"
          end
        end
      end
    end
  end
end
