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
          expect(User.shard(Shard.default).primary_shard).to eq Shard.default
          expect(User.shard(@shard1).primary_shard).to eq @shard1
        end

        it "should be the first shard of an array of shards" do
          expect(User.shard([Shard.default, @shard1]).primary_shard).to eq Shard.default
          expect(User.shard([@shard1, Shard.default]).primary_shard).to eq @shard1
        end

        it "should be the object's shard if it's a model" do
          expect(User.shard(@user1).primary_shard).to eq Shard.default
          expect(User.shard(@user2).primary_shard).to eq @shard1
        end

        it "should be the default shard if it's a scope of Shard" do
          expect(User.shard(::Rails.version < '4' ? Shard.scoped : Shard.all).primary_shard).to eq Shard.default
          @shard1.activate do
            expect(User.shard(::Rails.version < '4' ? Shard.scoped : Shard.all).primary_shard).to eq Shard.default
          end
        end
      end

      it "should default to the current shard" do
        relation = ::Rails.version < '4' ? User.scoped : User.all
        expect(relation.shard_value).to eq Shard.default
        expect(relation.shard_source_value).to eq :implicit

        @shard1.activate do
          expect(relation.shard_value).to eq Shard.default

          relation = ::Rails.version < '4' ? User.scoped : User.all
          expect(relation.shard_value).to eq @shard1
          expect(relation.shard_source_value).to eq :implicit
        end
        expect(relation.shard_value).to eq @shard1
      end

      describe "with primary key conditions" do
        it "should be changeable, and change conditions when it is changed" do
          relation = User.where(:id => @user1).shard(@shard1)
          expect(relation.shard_value).to eq @shard1
          expect(relation.shard_source_value).to eq :explicit
          expect(where_value(relation.where_values.first.right)).to eq @user1.global_id
        end

        it "should infer the shard from a single argument" do
          relation = User.where(:id => @user2)
          # execute on @shard1, with id local to that shard
          expect(relation.shard_value).to eq @shard1
          expect(where_value(relation.where_values.first.right)).to eq @user2.local_id
        end

        it "should infer the shard from multiple arguments" do
          relation = User.where(:id => [@user2, @user2])
          # execute on @shard1, with id local to that shard
          expect(relation.shard_value).to eq @shard1
          expect(where_value(relation.where_values.first.right)).to eq [@user2.local_id, @user2.local_id]
        end

        it "should infer the correct shard from an array of 1" do
          relation = User.where(:id => [@user2])
          # execute on @shard1, with id local to that shard
          expect(relation.shard_value).to eq @shard1
          expect(where_value(Array(relation.where_values.first.right))).to eq [@user2.local_id]
        end

        it "should do nothing when it's an array of 0" do
          relation = User.where(:id => [])
          # execute on @shard1, with id local to that shard
          expect(relation.shard_value).to eq Shard.default
          expect(where_value(relation.where_values.first.right)).to eq []
        end

        it "should order the shards preferring the shard it already had as primary" do
          relation = User.where(:id => [@user1, @user2])
          expect(relation.shard_value).to eq [Shard.default, @shard1]
          expect(where_value(relation.where_values.first.right)).to eq [@user1.local_id, @user2.global_id]

          @shard1.activate do
            relation = User.where(:id => [@user1, @user2])
            expect(relation.shard_value).to eq [@shard1, Shard.default]
            expect(where_value(relation.where_values.first.right)).to eq [@user1.global_id, @user2.local_id]
          end
        end
      end

      describe "with foreign key conditions" do
        it "should be changeable, and change conditions when it is changed" do
          relation = Appendage.where(:user_id => @user1)
          expect(relation.shard_value).to eq Shard.default
          expect(relation.shard_source_value).to eq :implicit
          expect(where_value(relation.where_values.first.right)).to eq @user1.local_id

          relation = relation.shard(@shard1)
          expect(relation.shard_value).to eq @shard1
          expect(relation.shard_source_value).to eq :explicit
          expect(where_value(relation.where_values.first.right)).to eq @user1.global_id
        end

        it "should translate ids based on current shard" do
          relation = Appendage.where(:user_id => [@user1, @user2])
          expect(where_value(relation.where_values.first.right)).to eq [@user1.local_id, @user2.global_id]

          @shard1.activate do
            relation = Appendage.where(:user_id => [@user1, @user2])
            expect(where_value(relation.where_values.first.right)).to eq [@user1.global_id, @user2.local_id]
          end
        end

        it "should translate ids in joins" do
          relation = User.joins(:appendage).where(appendages: { user_id: [@user1, @user2]})
          expect(where_value(relation.where_values.first.right)).to eq [@user1.local_id, @user2.global_id]
        end

        it "should translate ids according to the current shard of the foreign type" do
          @shard1.activate(:mirror_universe) do
            mirror_user = MirrorUser.create!
            relation = User.where(mirror_user_id: mirror_user)
            expect(where_value(relation.where_values.first.right)).to eq mirror_user.global_id
          end
        end
      end

      describe "with table aliases" do
        it "should properly construct the query" do
          child = @user1.children.create!
          grandchild = child.children.create!
          expect(child.reload.parent).to eq @user1

          relation = @user1.association(:grandchildren)
          relation = ::Rails.version < '4' ? relation.scoped : relation.scope

          attribute = relation.where_values.first.left
          expect(attribute.name.to_s).to eq 'parent_id'
          expect(attribute.relation.class).to eq ::Arel::Nodes::TableAlias

          rel, column = relation.send(:relation_and_column, attribute)
          expect(relation.send(:sharded_primary_key?, rel, column)).to eq false
          expect(relation.send(:sharded_foreign_key?, rel, column)).to eq true

          expect(@user1.grandchildren).to eq [grandchild]
        end
      end
    end
  end
end
