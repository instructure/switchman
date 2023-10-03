# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe FinderMethods do
      include RSpecHelper

      describe "#touch" do
        it "touches on the correct shard" do
          user = @shard1.activate { User.create! }

          old_time = 1.day.ago
          @shard1.activate do
            User.where(id: user).update_all(updated_at: old_time)
          end

          expect(user.reload.updated_at.to_i).to eq old_time.to_i
          user.touch
          expect(user.reload.updated_at.to_i).not_to eq old_time.to_i
        end
      end

      describe "#update_columns" do
        it "updates on the correct shard" do
          user = @shard1.activate { User.create! }

          new_time = 1.day.from_now
          expect(user.update_columns(updated_at: new_time)).to be true
          expect(user.reload.updated_at.to_i).to eq new_time.to_i
        end
      end

      describe "#reload" do
        it "loads canonical record as writable for shadow records" do
          real_digit = @shard2.activate { Digit.create! }
          real_digit.save_shadow_record(target_shard: @shard1)
          shadow_digit = @shard1.activate { Digit.find_by("id = ?", real_digit.global_id) }
          expect(shadow_digit).to be_shadow_record
          expect(shadow_digit).to be_readonly
          shadow_digit.reload
          expect(shadow_digit).not_to be_shadow_record
          expect(shadow_digit).not_to be_readonly
        end

        it "preserves explicit readonly" do
          real_digit = @shard2.activate { Digit.create! }
          real_digit.save_shadow_record(target_shard: @shard1)
          shadow_digit = @shard1.activate { Digit.find_by("id = ?", real_digit.global_id) }
          shadow_digit.readonly!
          expect(shadow_digit).to be_shadow_record
          expect(shadow_digit).to be_readonly
          shadow_digit.reload
          expect(shadow_digit).not_to be_shadow_record
          expect(shadow_digit).to be_readonly
        end
      end
    end
  end
end
