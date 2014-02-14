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

      it "should get the record size" do
        a1 = @user1.appendages.create!
        a2 = @user1.appendages.build
        @user1.appendages.size.should == 2
        @user1.reload
        @user1.appendages.size.should == 1
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

      it "should properly set up a cross-shard-category query" do
        @shard1.activate(:mirror_universe) do
          mirror_user = MirrorUser.create!
          relation = mirror_user.association(:user).scoped
          relation.shard_value.should == Shard.default
          relation.where_values.first.right.should == mirror_user.global_id
        end
      end

      describe "multishard associations" do
        it "should group has_many associations over associated_shards" do
          @shard1.activate{ Appendage.create!(:user_id => @user1, :value => 1) }
          @shard2.activate{ Appendage.create!(:user => @user1, :value => 2) }

          @user1.appendages.to_a.map(&:value).should == [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          @user1.appendages.to_a.map(&:value).sort.should == [1, 2]
        end

        it "follow shards for has_many :through" do
          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create!(:value => 1) }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          @user1.digits.to_a.map(&:value).should == [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          @user1.digits.to_a.map(&:value).sort.should == [1, 2]
        end

        it "should include the shard in scopes created by associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ Appendage.create!(:user_id => @user1, :value => 1) }
          @shard2.activate{ Appendage.create!(:user => @user1) }

          @user1.appendages.has_no_value.to_a.count.should == 1

          @user1.reload
          @shard2.activate {@user1.appendages.has_no_value.to_a.count.should == 1}
        end

        it "should include the shard in scopes created by has_many :through associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create! }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          @user1.digits.has_no_value.count.should == 1

          @user1.reload
          @shard2.activate {@user1.digits.has_no_value.to_a.count.should == 1}
        end

        it "should work with calculations in scopes created by associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ Appendage.create!(:user_id => @user1, :value => 1) }
          @shard2.activate{ Appendage.create!(:user => @user1); @user1.appendages.create!(:value => 2) }

          @user1.reload
          @user1.appendages.has_value.sum(:value).should == 3

          @user1.reload
          @shard2.activate {@user1.appendages.has_value.sum(:value).should == 3}
        end

        it "should work with calculations in scopes created by has_many :through associations" do
          @user1.associated_shards = [@shard1, @shard2]
          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create!; a1.digits.create!(:value => 1) }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          @user1.digits.has_value.sum(:value).should == 3
          @user1.reload
          @shard2.activate {@user1.digits.has_value.sum(:value).should == 3}
        end

        it "should be able to explicitly set the shard and still work with named scopes" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate{ a1 = Appendage.create!(:user_id => @user1); a1.digits.create! }
          @shard2.activate{ a2 = Appendage.create!(:user_id => @user1); a2.digits.create!(:value => 2) }

          @user1.digits.shard(@shard1).has_no_value.to_a.count.should == 1
          @user1.digits.shard(@shard2).has_no_value.to_a.count.should == 0

          @user1.reload

          @user1.digits.has_no_value.shard(@shard1).to_a.count.should == 1
          @user1.digits.has_no_value.shard(@shard2).to_a.count.should == 0
        end

        describe "belongs_to associations" do
          it "should identify an implied shard value based on the foreign id" do
            @shard1.activate do
              @appendage = Appendage.create!(:user_id => @user2.global_id)
              @appendage.reload.user.should == @user2
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
              copy.user.shard.should == @shard1
              copy.user.should == @user1
            end
          end
        end

        describe "preloading" do
          it "should preload belongs_to associations across shards" do
            a1 = Appendage.create!(:user => @user1)
            a2 = Appendage.create!(:user => @user2)
            user3 = User.create!
            user3.appendages.create!

            appendages = Appendage.all(:include => :user)
            appendages2 = Appendage.includes(:user).all
            @user1.delete

            appendages.map(&:user).sort.should == [@user1, @user2, user3].sort
            appendages2.map(&:user).sort.should == [@user1, @user2, user3].sort
          end

          it "should preload belongs_to :through associations across shards" do
            a1 = Appendage.create!(:user => @user1)
            d1 = a1.digits.create!

            a2 = @shard1.activate {Appendage.create!(:user => @user2) }
            d2 = Digit.create!(:appendage => a2)

            digits = Digit.includes(:user).all
            @user1.delete

            digits.map(&:user).sort.should == [@user1, @user2].sort
          end

          it "should preload has_many associations across associated shards" do
            a1 = @user1.appendages.create!
            a2 = @shard2.activate { Appendage.create!(:user_id => @user1) } # a2 will be in @user1's associated shards
            a3 = @shard1.activate { Appendage.create!(:user_id => @user2) } # a3 is not on @user2's associated shard

            User.associated_shards_map = { @user1.global_id => [@shard1, @shard2] }

            begin
              users = User.where(:id => [@user1, @user2]).includes(:appendages).all
              users.each {|u| u.appendages.loaded?.should be_true}

              u1 = users.detect {|u| u.id == @user1.id}
              u2 = users.detect {|u| u.id == @user2.id}

              a1.delete
              u1.appendages.sort.should == [a1, a2].sort
              u2.appendages.should be_empty
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
              users = User.where(:id => [@user1, @user2]).includes(:digits).all
              users.each {|u| u.digits.loaded?.should be_true}

              u1 = users.detect {|u| u.id == @user1.id}
              u2 = users.detect {|u| u.id == @user2.id}

              d1.delete

              u1.digits.sort.should == [d1, d2, d3].sort
              u2.digits.should == [d7]
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
            feature.owner.should == appendage
            feature.owner_id.should == appendage.id
            feature.owner_type.should == "Appendage"

            feature.owner = @user1
            feature.save!

            feature.reload
            feature.owner_id.should == @user1.global_id
            feature.owner_type.should == "User"
          end

          it "should work with multi-shard associations" do
            @shard1.activate{ Feature.create!(:owner => @user1, :value => 1) }
            @shard2.activate{ Feature.create!(:owner => @user1, :value => 2) }

            @user1.features.to_a.map(&:value).should == [1]

            @user1.reload
            @user1.associated_shards = [@shard1, @shard2]
            @user1.features.to_a.map(&:value).sort.should == [1, 2]
          end
        end
      end
    end
  end
end