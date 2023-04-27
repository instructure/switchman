# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe Calculations do
      include RSpecHelper

      describe "#pluck" do
        before do
          @shard1.activate do
            @user1 = User.create!(name: "user1")
            @appendage1 = @user1.appendages.create!
          end
          @shard2.activate do
            @user2 = User.create!(name: "user2")
            @appendage2 = @user2.appendages.create!
          end
        end

        it "returns non-id columns" do
          expect(User.where(id: [@user1.id, @user2.id]).pluck(:name).sort).to eq %w[user1 user2]
        end

        it "returns primary ids relative to current shard" do
          expect(Appendage.where(id: @appendage1).pluck(:id)).to eq [@appendage1.global_id]
          expect(Appendage.where(id: @appendage2).pluck(:id)).to eq [@appendage2.global_id]
          @shard1.activate do
            expect(Appendage.where(id: @appendage1).pluck(:id)).to eq [@appendage1.local_id]
            expect(Appendage.where(id: @appendage2).pluck(:id)).to eq [@appendage2.global_id]
          end
          @shard2.activate do
            expect(Appendage.where(id: @appendage1).pluck(:id)).to eq [@appendage1.global_id]
            expect(Appendage.where(id: @appendage2).pluck(:id)).to eq [@appendage2.local_id]
          end
        end

        it "returns foreign ids relative to current shard" do
          expect(Appendage.where(id: @appendage1).pluck(:user_id)).to eq [@user1.global_id]
          expect(Appendage.where(id: @appendage2).pluck(:user_id)).to eq [@user2.global_id]
          @shard1.activate do
            expect(Appendage.where(id: @appendage1).pluck(:user_id)).to eq [@user1.local_id]
            expect(Appendage.where(id: @appendage2).pluck(:user_id)).to eq [@user2.global_id]
          end
          @shard2.activate do
            expect(Appendage.where(id: @appendage1).pluck(:user_id)).to eq [@user1.global_id]
            expect(Appendage.where(id: @appendage2).pluck(:user_id)).to eq [@user2.local_id]
          end
        end

        it "post-uniqs multi-shard" do
          user3 = User.create!(name: "user2")
          expect(User.where(id: [@user1.id,
                                 @user2.id,
                                 user3.id]).distinct.pluck(:name).sort).to match_array %w[user1 user2]
        end

        it "works when setting an AR shard value" do
          expect(Appendage.shard(@user1).pluck(:user_id)).to eq [@user1.global_id]
        end

        it "works with multi-column plucking" do
          expect(Appendage.where(id: @appendage1).pluck(:id,
                                                        :user_id)).to eq [[@appendage1.global_id, @user1.global_id]]
          expect(Appendage.where(id: @appendage2).pluck(:id,
                                                        :user_id)).to eq [[@appendage2.global_id, @user2.global_id]]
          @shard1.activate do
            expect(Appendage.where(id: @appendage1).pluck(:id,
                                                          :user_id)).to eq [[@appendage1.local_id, @user1.local_id]]
            expect(Appendage.where(id: @appendage2).pluck(:id,
                                                          :user_id)).to eq [[@appendage2.global_id, @user2.global_id]]
          end
          @shard2.activate do
            expect(Appendage.where(id: @appendage1).pluck(:id,
                                                          :user_id)).to eq [[@appendage1.global_id, @user1.global_id]]
            expect(Appendage.where(id: @appendage2).pluck(:id,
                                                          :user_id)).to eq [[@appendage2.local_id, @user2.local_id]]
          end
        end
      end

      describe "#execute_simple_calculation" do
        before do
          @appendages = []
          @shard1.activate do
            @user1 = User.create!(name: "user1")
            @appendages << @user1.appendages.create!(value: 1)
            @appendages << @user1.appendages.create!(value: 2)
          end
          @shard2.activate do
            @user2 = User.create!(name: "user2")
            @appendages << @user2.appendages.create!(value: 3)
            @appendages << @user2.appendages.create!(value: 4)
            @appendages << @user2.appendages.create!(value: 5)
          end
        end

        it "calculates average across shards" do
          expect(@user1.appendages.average(:value)).to eq 1.5
          expect(@shard1.activate { Appendage.average(:value) }).to eq 1.5

          expect(@user2.appendages.average(:value)).to eq 4
          expect(@shard2.activate { Appendage.average(:value) }).to eq 4

          expect(Appendage.where(id: @appendages).average(:value)).to eq 3
        end

        it "counts across shards" do
          expect(@user1.appendages.count).to eq 2
          expect(@shard1.activate { Appendage.count }).to eq 2

          expect(@user2.appendages.count).to eq 3
          expect(@shard2.activate { Appendage.count }).to eq 3

          expect(Appendage.where(id: @appendages).count).to eq 5
        end

        it "calculates minimum across shards" do
          expect(@user1.appendages.minimum(:value)).to eq 1
          expect(@shard1.activate { Appendage.minimum(:value) }).to eq 1

          expect(@user2.appendages.minimum(:value)).to eq 3
          expect(@shard2.activate { Appendage.minimum(:value) }).to eq 3

          expect(Appendage.where(id: @appendages).minimum(:value)).to eq 1
        end

        it "calculates maximum across shards" do
          expect(@user1.appendages.maximum(:value)).to eq 2
          expect(@shard1.activate { Appendage.maximum(:value) }).to eq 2

          expect(@user2.appendages.maximum(:value)).to eq 5
          expect(@shard2.activate { Appendage.maximum(:value) }).to eq 5

          expect(Appendage.where(id: @appendages).maximum(:value)).to eq 5
        end

        it "works with dates across shards" do
          expect(Appendage.where(id: @appendages).maximum(:created_at).to_i)
            .to eq @appendages.map(&:created_at).map(&:to_i).max
        end

        it "calculates sum across shards" do
          expect(@user1.appendages.sum(:value)).to eq 3
          expect(@shard1.activate { Appendage.sum(:value) }).to eq 3

          expect(@user2.appendages.sum(:value)).to eq 12
          expect(@shard2.activate { Appendage.sum(:value) }).to eq 12

          expect(Appendage.where(id: @appendages).sum(:value)).to eq 15
        end
      end

      describe "#execute_grouped_calculation" do
        before do
          @appendages = []
          @shard1.activate { @user1 = User.create!(name: "user1") }
          @shard2.activate { @user2 = User.create!(name: "user2") }

          @shard1.activate do
            @appendages << Appendage.create!(user_id: @user1.id, value: 1)
            @appendages << Appendage.create!(user_id: @user2.id, value: 3)
          end
          @shard2.activate do
            @appendages << Appendage.create!(user_id: @user1.id, value: 2)
            @appendages << Appendage.create!(user_id: @user2.id, value: 4)
            @appendages << Appendage.create!(user_id: @user2.id, value: 5)
          end
        end

        it "calculates an expression with uppercase values across shards" do
          @shard1.activate do
            result = Appendage.group("(created_at::TIMESTAMPTZ AT TIME ZONE 'UTC')::DATE").count
            expect(result.keys.length).to eq 1
            expect(result.keys.first).not_to be_nil
          end
        end

        it "calculates average across shards" do
          expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").average(:value)).to eq(
            { @user1.global_id => 1.5, @user2.global_id => 4 }
          )

          @shard1.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").average(:value)).to eq(
              { @user1.local_id => 1.5, @user2.global_id => 4 }
            )
          end

          @shard2.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").average(:value)).to eq(
              { @user1.global_id => 1.5, @user2.local_id => 4 }
            )
          end

          expect(Appendage.shard([@shard1, @shard2]).group(:user).average(:value)).to eq(
            { @user1 => 1.5, @user2 => 4 }
          )
        end

        it "counts across shards" do
          expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").count).to eq(
            { @user1.global_id => 2, @user2.global_id => 3 }
          )

          @shard1.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").count).to eq(
              { @user1.local_id => 2, @user2.global_id => 3 }
            )
          end

          @shard2.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").count).to eq(
              { @user1.global_id => 2, @user2.local_id => 3 }
            )
          end

          expect(Appendage.shard([@shard1, @shard2]).group(:user).count).to eq(
            { @user1 => 2, @user2 => 3 }
          )
        end

        it "calculates minimum across shards" do
          expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").minimum(:value)).to eq(
            { @user1.global_id => 1, @user2.global_id => 3 }
          )

          @shard1.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").minimum(:value)).to eq(
              { @user1.local_id => 1, @user2.global_id => 3 }
            )
          end

          @shard2.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").minimum(:value)).to eq(
              { @user1.global_id => 1, @user2.local_id => 3 }
            )
          end

          expect(Appendage.shard([@shard1, @shard2]).group(:user).minimum(:value)).to eq(
            { @user1 => 1, @user2 => 3 }
          )
        end

        it "calculates maximum across shards" do
          expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").maximum(:value)).to eq(
            { @user1.global_id => 2, @user2.global_id => 5 }
          )

          @shard1.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").maximum(:value)).to eq(
              { @user1.local_id => 2, @user2.global_id => 5 }
            )
          end

          @shard2.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").maximum(:value)).to eq(
              { @user1.global_id => 2, @user2.local_id => 5 }
            )
          end

          expect(Appendage.shard([@shard1, @shard2]).group(:user).maximum(:value)).to eq(
            { @user1 => 2, @user2 => 5 }
          )
        end

        it "calculates sum across shards" do
          expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").sum(:value)).to eq(
            { @user1.global_id => 3, @user2.global_id => 12 }
          )

          @shard1.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").sum(:value)).to eq(
              { @user1.local_id => 3, @user2.global_id => 12 }
            )
          end

          @shard2.activate do
            expect(Appendage.shard([@shard1, @shard2]).group("appendages.user_id").sum(:value)).to eq(
              { @user1.global_id => 3, @user2.local_id => 12 }
            )
          end

          expect(Appendage.shard([@shard1, @shard2]).group(:user).sum(:value)).to eq(
            { @user1 => 3, @user2 => 12 }
          )
        end

        it "respects order for a single shard" do
          @shard1.activate do
            @user1.appendages.create!
            user2 = User.create!
            user2.appendages.create!
            expect(Appendage.group(:user_id).order(::Arel.sql("COUNT(*) DESC")).limit(1).count)
              .to eq({ @user1.id => 2 })
          end
        end

        it "is able to group by joined columns with qualified names" do
          user = User.create!
          user.appendages.create!

          expect(Appendage.joins(:user).group(:mirror_user_id).count).to eq({ nil => 1 })
        end
      end
    end
  end
end
