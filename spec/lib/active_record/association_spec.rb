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
        expect(a1.shard).to eq @shard1
      end

      it "should associate created objects with parent shard" do
        a1 = @user1.appendages.create!
        expect(a1.shard).to eq @shard1
      end

      it "should set shard value to parent for association scope" do
        scope = @user1.appendages
        scope = ::Rails.version < '4' ? scope.scoped : scope.scope
        expect(scope.shard_value).to eq @user1
        expect(scope.shard_source_value).to eq :association
      end

      it "should find by id through association" do
        a1 = @user1.appendages.create!

        expect(@user1.appendages.find(a1.id)).to eq a1
        expect { @user2.appendages.find(a1.id) }.to raise_exception(::ActiveRecord::RecordNotFound)
      end

      describe "transaction" do
        it "should activate the owner's shard and start the transaction on that shard" do
          base_value = @user1.shard.activate { User.connection.open_transactions }
          @user1.appendages.transaction(:requires_new => true) do
            expect(Shard.current).to eq @shard1
            expect(User.connection.open_transactions).to eq base_value + 1
          end
        end
      end

      it "should get the record size" do
        a1 = @user1.appendages.create!
        a2 = @user1.appendages.build
        expect(@user1.appendages.size).to eq 2
        @user1.reload
        expect(@user1.appendages.size).to eq 1
      end

      it "should reverse the association" do
        a1 = @user1.appendages.create!
        a1.reload
        expect(a1.user.shard).to eq @shard1
        expect(a1.user).to eq @user1
      end

      it "should work with has_many through associations" do
        a1 = @user1.appendages.create!
        d1 = a1.digits.create!
        expect(d1.shard).to eq @shard1

        if ::Rails.version < '4'
          expect(@user1.digits.scoped.shard_value).to eq @user1
        else
          expect(@user1.digits.scope.shard_value).to eq @user1
        end
        expect(@user1.digits.find(d1.id)).to eq d1
      end

      it "should resolve include? correctly for a has_many :through" do
        @shard1.activate do
          child = @user1.children.create!
          @grandchild = child.children.create!
          @user1.reload
          expect(@user1.grandchildren.loaded?).to eq false
          expect(@user1.grandchildren.include?(@grandchild)).to eq true
        end
        @shard2.activate do
          fake = User.create!(id: @grandchild.local_id)
          @user1.reload
          expect(@user1.grandchildren.loaded?).to eq false
          expect(@user1.grandchildren.include?(fake)).to eq false
        end
      end

      it "shard should be changeable, and change conditions when it is changed" do
        a1 = @user1.appendages.create!
        relation = @user1.appendages.where(:id => a1).shard(@shard1)
        expect(relation.shard_value).to eq @shard1
        expect(relation.shard_source_value).to eq :explicit
        expect(relation.where_values.detect{|v| v.left.name == "id"}.right).to eq a1.local_id

        relation = @user1.appendages.where(:id => a1).shard(@shard2)
        expect(relation.shard_value).to eq @shard2
        expect(relation.shard_source_value).to eq :explicit
        expect(relation.where_values.detect{|v| v.left.name == "id"}.right).to eq a1.global_id
      end

      it "should transpose predicates correctly" do
        a1 = @user1.appendages.create!
        a2 = @user2.appendages.create!

        relation = @user1.appendages.where(:id => a2)
        expect(relation.shard_value).to eq @user1
        expect(relation.where_values.detect{|v| v.left.name == "id"}.right).to eq a2.global_id

        relation = @user1.appendages.where(:id => [a1, a2])
        expect(relation.shard_value).to eq @user1
        expect(relation.where_values.detect{|v| v.left.name == "id"}.right).to eq [a1.local_id, a2.global_id]
      end

      it "should properly set up a cross-shard-category query" do
        @shard1.activate(:mirror_universe) do
          mirror_user = MirrorUser.create!
          relation = mirror_user.association(:user)
          relation = ::Rails.version < '4' ? relation.scoped : relation.scope
          expect(relation.shard_value).to eq Shard.default
          if ::Rails.version < '4'
            expect(relation.where_values.first.right).to eq mirror_user.global_id
          else
            expect(relation.where_values.first.right).to be_a(Arel::Nodes::BindParam)
            expect(relation.bind_values.map(&:last)).to eq [mirror_user.global_id]
          end
        end
      end

      describe "multishard associations" do
        it "should group has_many associations over associated_shards" do
          @shard1.activate{ Appendage.create!(:user_id => @user1, :value => 1) }
          @shard2.activate{ Appendage.create!(:user => @user1, :value => 2) }

          expect(@user1.appendages.to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          expect(@user1.appendages.to_a.map(&:value).sort).to eq [1, 2]
        end

        it "follow shards for has_many :through" do
          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create!(:value => 1) }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          expect(@user1.digits.to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          expect(@user1.digits.to_a.map(&:value).sort).to eq [1, 2]
        end

        it "should include the shard in scopes created by associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ Appendage.create!(:user_id => @user1, :value => 1) }
          @shard2.activate{ Appendage.create!(:user => @user1) }

          expect(@user1.appendages.has_no_value.to_a.count).to eq 1

          @user1.reload
          @shard2.activate {expect(@user1.appendages.has_no_value.to_a.count).to eq 1}
        end

        it "should include the shard in scopes created by has_many :through associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create! }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          expect(@user1.digits.has_no_value.count).to eq 1

          @user1.reload
          @shard2.activate {expect(@user1.digits.has_no_value.to_a.count).to eq 1}
        end

        it "should work with calculations in scopes created by associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ Appendage.create!(:user_id => @user1, :value => 1) }
          @shard2.activate{ Appendage.create!(:user => @user1); @user1.appendages.create!(:value => 2) }

          @user1.reload
          expect(@user1.appendages.has_value.sum(:value)).to eq 3

          @user1.reload
          @shard2.activate {expect(@user1.appendages.has_value.sum(:value)).to eq 3}
        end

        it "should work with calculations in scopes created by has_many :through associations" do
          @user1.associated_shards = [@shard1, @shard2]
          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create!; a1.digits.create!(:value => 1) }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          expect(@user1.digits.has_value.sum(:value)).to eq 3
          @user1.reload
          @shard2.activate {expect(@user1.digits.has_value.sum(:value)).to eq 3}
        end

        it "should be able to explicitly set the shard and still work with named scopes" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create! }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          expect(@user1.digits.shard(@shard1).has_no_value.to_a.count).to eq 1
          expect(@user1.digits.shard(@shard2).has_no_value.to_a.count).to eq 0

          @user1.reload

          expect(@user1.digits.has_no_value.shard(@shard1).to_a.count).to eq 1
          expect(@user1.digits.has_no_value.shard(@shard2).to_a.count).to eq 0
        end

        describe "unsharded associations" do
          it "should be able to create an unsharded new record through a collection" do
            root = @user2.roots.create!
            root.reload
            expect(root.shard).to eq Shard.default
            expect(root.user_id).to eq @user2.global_id
            expect(root.user).to eq @user2
          end
        end

        describe "belongs_to associations" do
          it "should identify an implied shard value based on the foreign id" do
            @shard1.activate do
              @appendage = Appendage.create!(:user_id => @user2.global_id)
              expect(@appendage.reload.user).to eq @user2
            end
          end

          it "should translate foreign keys when replacing the record" do
            a = @shard2.activate { Appendage.create! }
            copy = a.dup
            @shard1.activate do
              copy.user = @user1
              copy.shard = @user1.shard
              copy.save!
              copy.reload
              expect(copy.user.shard).to eq @shard1
              expect(copy.user).to eq @user1
            end
          end
        end

        describe "preloading" do
          it "should preload belongs_to associations across shards" do
            a1 = Appendage.create!(:user => @user1)
            a2 = Appendage.create!(:user => @user2)
            user3 = User.create!
            user3.appendages.create!

            appendages = Appendage.includes(:user).to_a
            @user1.delete

            expect(appendages.map(&:user).sort).to eq [@user1, @user2, user3].sort
          end

          it "should preload belongs_to :through associations across shards" do
            a1 = Appendage.create!(:user => @user1)
            d1 = a1.digits.create!

            a2 = @shard1.activate {Appendage.create!(:user => @user2) }
            d2 = Digit.create!(:appendage => a2)

            digits = Digit.includes(:user).to_a
            @user1.delete

            expect(digits.map(&:user).sort).to eq [@user1, @user2].sort
          end

          it "should preload has_many associations across associated shards" do
            a1 = @user1.appendages.create!
            a2 = @shard2.activate { Appendage.create!(:user_id => @user1) } # a2 will be in @user1's associated shards
            a3 = @shard1.activate { Appendage.create!(:user_id => @user2) } # a3 is not on @user2's associated shard

            User.associated_shards_map = { @user1.global_id => [@shard1, @shard2] }

            begin
              users = User.where(:id => [@user1, @user2]).includes(:appendages).to_a
              users.each {|u| expect(u.appendages.loaded?).to eq true}

              u1 = users.detect {|u| u.id == @user1.id}
              u2 = users.detect {|u| u.id == @user2.id}

              a1.delete
              expect(u1.appendages.sort).to eq [a1, a2].sort
              expect(u2.appendages).to be_empty
            ensure
              User.associated_shards_map = nil
            end
          end

          it "should preload has_many :through associations across associated shards" do
            a1 = @user1.appendages.create!
            a2 = @shard2.activate { Appendage.create!(:user_id => @user1) }
            a3 = @shard2.activate { Appendage.create!(:user_id => @user1) }

            d1 = a1.digits.create!
            d2 = a2.digits.create! # a2 will be in @user1's associated shards
            d3 = @shard1.activate { Digit.create!(:appendage_id => a2) } # d3 will be in a2's associated shards
            d4 = @shard1.activate { Digit.create!(:appendage_id => a3) } # d4 is not on a3's shard

            a4 = @shard1.activate { Appendage.create!(:user_id => @user2) }
            a5 = @user2.appendages.create!
            a6 = @user2.appendages.create!

            d5 = @shard2.activate { Digit.create!(:appendage_id => a4) } # d5 is on @user2's shard but a4 is not
            d6 = @shard1.activate { Digit.create!(:appendage_id => a5) } # a5 is on @user2's shard but d6 is not
            d7 = @shard1.activate { Digit.create!(:appendage_id => a6) } # d7 will be in a6's associated shards

            User.associated_shards_map = { @user1.global_id => [@shard1, @shard2] }
            Appendage.associated_shards_map = { a2.global_id => [@shard1, @shard2], a6.global_id => [@shard1] }

            begin
              users = User.where(:id => [@user1, @user2]).includes(:digits).to_a
              users.each {|u| expect(u.digits.loaded?).to eq true}

              u1 = users.detect {|u| u.id == @user1.id}
              u2 = users.detect {|u| u.id == @user2.id}

              d1.delete

              expect(u1.digits.sort).to eq [d1, d2, d3].sort
              expect(u2.digits).to eq [d7]
            ensure
              User.associated_shards_map = nil
              Appendage.associated_shards_map = nil
            end
          end
        end

        describe "polymorphic associations" do
          it "should work normally" do
            appendage = Appendage.create!
            feature = Feature.create!(:owner => appendage)

            feature.reload
            expect(feature.owner).to eq appendage
            expect(feature.owner_id).to eq appendage.id
            expect(feature.owner_type).to eq "Appendage"

            feature.owner = @user1
            feature.save!

            feature.reload
            expect(feature.owner_id).to eq @user1.global_id
            expect(feature.owner_type).to eq "User"
          end

          it "should work with multi-shard associations" do
            @shard1.activate{ Feature.create!(:owner => @user1, :value => 1) }
            @shard2.activate{ Feature.create!(:owner => @user1, :value => 2) }

            expect(@user1.features.to_a.map(&:value)).to eq [1]

            @user1.reload
            @user1.associated_shards = [@shard1, @shard2]
            expect(@user1.features.to_a.map(&:value).sort).to eq [1, 2]
          end
        end
      end
    end
  end
end