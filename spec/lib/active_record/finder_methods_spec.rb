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
          expect(User.find(@user.global_id)).to eq @user
        end

        it "should find with a global id and a current scope" do
          User.where("id > 0").scoping do
            # having a current scope skips the statement cache in rails 4.2
            expect(User.find(@user.global_id)).to eq @user
          end
        end
      end

      describe "#find_by_attributes" do
        it "should find with a global id" do
          expect(User.find_by_id(@user.global_id)).to eq @user
        end

        it "should find with an array of global ids" do
          expect(User.find_by_id([@user.global_id])).to eq @user
        end
      end

      describe "#find_or_initialize" do
        it "should initialize with the shard from the scope" do
          @user.destroy
          u = User.shard(@shard1).where(id: @user).first_or_initialize
          expect(u).to be_new_record
          expect(u.shard).to eq @shard1
        end
      end

      describe "#exists?" do
        it "should work for an out-of-shard scope" do
          scope = @shard1.activate { User.where(id: @user) }
          expect(scope.shard_value).to eq @shard1
          expect(scope.exists?).to eq true
        end

        it "should work for a multi-shard scope" do
          user2 = @shard2.activate { User.create!(name: "multi-shard exists") }
          expect(User.where(name: "multi-shard exists").shard(::Rails.version < '4' ? Shard.scoped : Shard.all).exists?).to eq true
        end
      end
    end
  end
end
