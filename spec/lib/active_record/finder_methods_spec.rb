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

        it "should be able to find by global id with qualified names" do
          User.connection.stubs(:use_qualified_names?).returns(true)

          other_user = User.create!
          expect(User.find(other_user.id)).to eq other_user # make sure we're not caching a statement from the wrong shard

          expect(User.find(@user.global_id)).to eq @user
        end

        it "should find digit with a global id on an association" do
          appendage = @shard2.activate { Appendage.create!(user: @user) }
          digit = appendage.digits.create!
          @user.associated_shards = [@shard1, @shard2]
          expect(@user.digits.find(digit.global_id)).to eq digit
        end

        it "doesn't break non-integral primary keys" do
          pv = PageView.create!(request_id: 'abc')
          expect(PageView.shard(Shard.default).find('abc')).to eq pv
        end

        it "doesn't break with bogus id" do
          expect { User.shard(Shard.default).find('bogus') }.to raise_error(::ActiveRecord::RecordNotFound)
        end
      end

      context "#find_last" do
        it "should work across shards with qualified names" do
          User.connection.stubs(:use_qualified_names?).returns(true)

          @shard1.activate do
            User.create!
            @user = User.create!
          end
          expect(User.shard(@shard1).last).to eq @user
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

      describe "#find_some" do
        it "should find multiple objects by global id" do
          user = User.create!
          user2 = @shard2.activate { User.create! }
          expect(User.find([user.global_id, user2.global_id]).sort_by(&:id)).to eq [user, user2].sort_by(&:id)
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
          expect(User.where(name: "multi-shard exists").shard(Shard.all).exists?).to eq true
        end

        it "should work for a multi-shard association scope" do
          @user = User.create!
          @shard1.activate { Appendage.create!(:user_id => @user.id)}
          expect(@user.appendages.shard([Shard.default, @shard1]).exists?).to eq true
        end

        it "should work if a condition is passed" do
          expect(User.exists?(@user.global_id)).to eq true
        end
      end
    end
  end
end
