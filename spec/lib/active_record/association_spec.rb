require "spec_helper"

module Switchman
  module ActiveRecord
    describe Association do
      include RSpecHelper

      before do
        @shard1.activate do
          @user1 = User.create!
        end
        @shard2.activate do
          @user2 = User.create!
        end
      end

      it "should associate built objects with parent shard" do
        a1 = @user1.appendages.build
        a1.shard.should == @shard1
      end

      it "should associate created objects with parent shard" do
        a1 = @user1.appendages.create!
        a1.shard.should == @shard1
      end

      it "should set shard value to parent for association scope" do
        scope = @user1.appendages.scoped
        scope.shard_value.should == @user1
        scope.shard_source_value.should == :association
      end

      it "should find by id through association" do
        a1 = @user1.appendages.create!

        @user1.appendages.find(a1.id).should == a1
        lambda { @user2.appendages.find(a1.id) }.should raise_exception(::ActiveRecord::RecordNotFound)
      end

      describe "transaction" do
        it "should activate the owner's shard and start the transaction on that shard" do
          @user1.appendages.transaction(:requires_new => true) do
            Shard.current.should == @shard1
            User.connection.open_transactions.should == 2
          end
        end
      end

      it "should reverse the association" do
        a1 = @user1.appendages.create!
        a1.reload
        a1.user.shard.should == @shard1
        a1.user.should == @user1
      end

      it "should work with has_many through associations" do
        a1 = @user1.appendages.create!
        d1 = a1.digits.create!
        d1.shard.should == @shard1

        @user1.digits.scoped.shard_value.should == @user1
        @user1.digits.find(d1.id).should == d1
      end

      it "shard should be changeable, and change conditions when it is changed" do
        a1 = @user1.appendages.create!
        relation = @user1.appendages.where(:id => a1).shard(@shard1)
        relation.shard_value.should == @shard1
        relation.shard_source_value.should == :explicit
        relation.where_values.detect{|v| v.left.name == "id"}.right.should == a1.local_id

        relation = @user1.appendages.where(:id => a1).shard(@shard2)
        relation.shard_value.should == @shard2
        relation.shard_source_value.should == :explicit
        relation.where_values.detect{|v| v.left.name == "id"}.right.should == a1.global_id
      end

      it "should transpose predicates correctly" do
        a1 = @user1.appendages.create!
        a2 = @user2.appendages.create!

        relation = @user1.appendages.where(:id => a2)
        relation.shard_value.should == @user1
        relation.where_values.detect{|v| v.left.name == "id"}.right.should == a2.global_id

        relation = @user1.appendages.where(:id => [a1, a2])
        relation.shard_value.should == @user1
        relation.where_values.detect{|v| v.left.name == "id"}.right.should == [a1.local_id, a2.global_id]
      end
    end
  end
end