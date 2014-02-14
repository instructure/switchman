require "spec_helper"

module Switchman
  module ActiveRecord
    describe QueryMethods do
      include RSpecHelper

      before do
        @user1 = User.create!
        @appendage1 = @user1.appendages.create!
        @user2 = @shard1.activate { User.create! }
        @appendage2 = @user2.appendages.create!
        @user3 = @shard2.activate { User.create! }
        @appendage3 = @user3.appendages.create!
      end

      describe "#primary_shard" do
        it "should be the shard if it's a shard" do
          User.shard(Shard.default).primary_shard.should == Shard.default
          User.shard(@shard1).primary_shard.should == @shard1
        end

        it "should be the first shard of an array of shards" do
          User.shard([Shard.default, @shard1]).primary_shard.should == Shard.default
          User.shard([@shard1, Shard.default]).primary_shard.should == @shard1
        end

        it "should be the object's shard if it's a model" do
          User.shard(@user1).primary_shard.should == Shard.default
          User.shard(@user2).primary_shard.should == @shard1
        end

        it "should be the default shard if it's a scope of Shard" do
          User.shard(Shard.scoped).primary_shard.should == Shard.default
          @shard1.activate do
            User.shard(Shard.scoped).primary_shard.should == Shard.default
          end
        end
      end

      it "should default to the current shard" do
        relation = User.scoped
        relation.shard_value.should == Shard.default
        relation.shard_source_value.should == :implicit

        @shard1.activate do
          relation.shard_value.should == Shard.default

          relation = User.scoped
          relation.shard_value.should == @shard1
          relation.shard_source_value.should == :implicit
        end
        relation.shard_value.should == @shard1
      end

      describe "with primary key conditions" do
        it "should be changeable, and change conditions when it is changed" do
          relation = User.where(:id => @user1).shard(@shard1)
          relation.shard_value.should == @shard1
          relation.shard_source_value.should == :explicit
          relation.where_values.first.right.should == @user1.global_id
        end

        it "should infer the shard from a single argument" do
          relation = User.where(:id => @user2)
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == @shard1
          relation.where_values.first.right.should == @user2.local_id
        end

        it "should infer the shard from multiple arguments" do
          relation = User.where(:id => [@user2, @user2])
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == @shard1
          relation.where_values.first.right.should == [@user2.local_id, @user2.local_id]
        end

        it "should infer the correct shard from an array of 1" do
          relation = User.where(:id => [@user2])
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == @shard1
          relation.where_values.first.right.should == [@user2.local_id]
        end

        it "should do nothing when it's an array of 0" do
          relation = User.where(:id => [])
          # execute on @shard1, with id local to that shard
          relation.shard_value.should == Shard.default
          relation.where_values.first.right.should == []
        end

        it "should order the shards preferring the shard it already had as primary" do
          relation = User.where(:id => [@user1, @user2])
          relation.shard_value.should == [Shard.default, @shard1]
          relation.where_values.first.right.should == [@user1.local_id, @user2.global_id]

          @shard1.activate do
            relation = User.where(:id => [@user1, @user2])
            relation.shard_value.should == [@shard1, Shard.default]
            relation.where_values.first.right.should == [@user1.global_id, @user2.local_id]
          end
        end
      end

      describe "with foreign key conditions" do
        it "should be changeable, and change conditions when it is changed" do
          relation = Appendage.where(:user_id => @user1)
          relation.shard_value.should == Shard.default
          relation.shard_source_value.should == :implicit
          relation.where_values.first.right.should == @user1.local_id

          relation = relation.shard(@shard1)
          relation.shard_value.should == @shard1
          relation.shard_source_value.should == :explicit
          relation.where_values.first.right.should == @user1.global_id
        end

        it "should translate ids based on current shard" do
          relation = Appendage.where(:user_id => [@user1, @user2])
          relation.where_values.first.right.should == [@user1.local_id, @user2.global_id]

          @shard1.activate do
            relation = Appendage.where(:user_id => [@user1, @user2])
            relation.where_values.first.right.should == [@user1.global_id, @user2.local_id]
          end
        end

        it "should translate ids in joins" do
          relation = User.joins(:appendage).where(appendages: { user_id: [@user1, @user2]})
          relation.where_values.first.right.should == [@user1.local_id, @user2.global_id]
        end

        it "should translate ids according to the current shard of the foreign type" do
          @shard1.activate(:mirror_universe) do
            mirror_user = MirrorUser.create!
            relation = User.where(mirror_user_id: mirror_user)
            relation.where_values.first.right.should == mirror_user.global_id
          end
        end
      end

      describe "with table aliases" do
        it "should properly construct the query" do
          child = @user1.children.create!
          grandchild = child.children.create!
          child.reload.parent.should == @user1

          relation = @user1.association(:grandchildren).scoped

          attribute = relation.where_values.first.left
          attribute.name.to_s.should == 'parent_id'
          attribute.relation.class.should == Arel::Nodes::TableAlias

          rel, column = relation.send(:relation_and_column, attribute)
          relation.send(:sharded_primary_key?, rel, column).should == false
          relation.send(:sharded_foreign_key?, rel, column).should == true

          @user1.grandchildren.should == [grandchild]
        end
      end
    end
  end
end
