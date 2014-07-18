require "spec_helper"

module Switchman
  module ActiveRecord
    describe Relation do
      include RSpecHelper

      before do
        @user1 = User.create!(:name => 'user1')
        @user2 = @shard1.activate { User.create!(:name => 'user2') }
      end

      describe "#exec_queries" do
        it "should activate the correct shard for the query" do
          expect(User.shard(@shard1).where(:id => @user2.local_id).first).to eq @user2
        end

        it "should activate multiple shards if necessary" do
          expect(User.where(:id => [@user1.id, @user2.id]).sort_by(&:id)).to eq [@user1, @user2].sort_by(&:id)
        end
      end

      describe "#update_all" do
        it "should activate the correct shard for the query" do
          User.shard(@shard1).where(:id => @user2.local_id).update_all(:name => 'a')
          expect(@user1.reload.name).to eq 'user1'
          expect(@user2.reload.name).to eq 'a'
        end

        it "should activate multiple shards if necessary" do
          User.where(:id => [@user1.id, @user2.id]).update_all(:name => 'a')
          expect(@user1.reload.name).to eq 'a'
          expect(@user2.reload.name).to eq 'a'
        end
      end

      describe "#new" do
        it "should infer the scope's shard" do
          scope = @shard1.activate { User.where(id: 1) }
          u = scope.new
          expect(u.shard).to eq @shard1
          expect(u.local_id).to eq 1
        end
      end
    end
  end
end
