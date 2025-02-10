# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe Associations do
      include RSpecHelper

      before do
        @shard1.activate do
          @user1 = User.create!
        end
        @shard2.activate do
          @user2 = User.create!
        end
      end

      it "correctly associates unsharded objects with unsharded objects" do
        root = Root.create!(user: @user1)
        app = Application.create!(root: root)
        all_apps = Application.all.includes(:root).to_a
        expect(all_apps.length).to eq 1
        expect(all_apps[0].id).to eq(app.id)
        expect(all_apps[0].root.id).to eq(root.id)
      end

      it "correctly associates unsharded objects with sharded objects" do
        root = Root.create!(user: @user1)
        users = @shard1.activate do
          User.all.includes(:roots).to_a
        end
        expect(users.length).to eq 1
        expect(users[0].id).to eq(@user1.id)
        expect(users[0].roots).to contain_exactly(root)
      end

      it "associates built objects with parent shard" do
        a1 = @user1.appendages.build
        expect(a1.shard).to eq @shard1
      end

      it "associates created objects with parent shard" do
        a1 = @user1.appendages.create!
        expect(a1.shard).to eq @shard1
      end

      it "sets shard value to parent for association scope" do
        scope = @user1.appendages.scope
        expect(scope.shard_value).to eq @user1
        expect(scope.shard_source_value).to eq :association
      end

      it "finds by id through association" do
        a1 = @user1.appendages.create!

        expect(@user1.appendages.find(a1.id)).to eq a1
        expect { @user2.appendages.find(a1.id) }.to raise_exception(::ActiveRecord::RecordNotFound)
      end

      it "doesn't choke instantiating a renamed has_many :through" do
        a = @user1.appendages.create!
        d = a.digits.create!
        expect(@user1.renamed_digits.to_a).to eq [d]
      end

      it "handles querying by the target of a has_many" do
        a = @user1.appendages.create!
        d = a.digits.create!
        # Ensure this test doesn't pass by accidnet
        d = a.digits.create! if a.id == d.id
        expect(a.id).not_to eq(d.id)

        expect(Appendage.where(digits: d)).to eq [a]
      end

      it "transposes ids correctly when using AR objects as query params" do
        a1 = @user1.appendages.create!

        expect(Appendage.where(id: a1.id, user_id: @user1).first).to eq a1
        expect(Appendage.where(id: a1.id, user: @user1).first).to eq a1
      end

      describe "transaction" do
        it "activates the owner's shard and start the transaction on that shard" do
          base_value = @user1.shard.activate { User.connection.open_transactions }
          @user1.appendages.transaction(requires_new: true) do
            expect(Shard.current).to eq @shard1
            expect(User.connection.open_transactions).to eq base_value + 1
          end
        end
      end

      it "gets the record size" do
        @user1.appendages.create!
        @user1.appendages.build
        expect(@user1.appendages.size).to eq 2
        @user1.reload
        expect(@user1.appendages.size).to eq 1
      end

      it "reverses the association" do
        a1 = @user1.appendages.create!
        a1.reload
        expect(a1.user.shard).to eq @shard1
        expect(a1.user).to eq @user1
      end

      it "uses the shard as part of the association_scope_cache key" do
        @user1.appendages.to_a # trigger the cache
        @user2.appendages.to_a # trigger the cache

        reflection = User.reflect_on_association("appendages")
        keys = Appendage.instance_variable_get(:@find_by_statement_cache)[User.connection.prepared_statements].keys
        expect(keys).to include([reflection, @user1.shard.id], [reflection, @user2.shard.id])
      end

      it "properly saves a new child STI object onto the parent's shard" do
        user = User.create!
        expect(@shard1.activate { user.appendages.create!(type: "Arm") }.shard).to eq user.shard
      end

      it "uses the target's shard category's shard as part of the association_scope_cache key" do
        @user1.roots.to_a # trigger the cache
        @user2.roots.to_a # trigger the cache

        reflection = User.reflect_on_association("roots")
        keys = Root.instance_variable_get(:@find_by_statement_cache)[User.connection.prepared_statements].keys
        expect(keys).to eq [[reflection, Shard.default.id]]
      end

      it "works with has_many through associations" do
        a1 = @user1.appendages.create!
        d1 = a1.digits.create!
        expect(d1.shard).to eq @shard1

        expect(@user1.digits.scope.shard_value).to eq @user1
        expect(@user1.digits.find(d1.id)).to eq d1
      end

      it "returns the real record when a shadow record is reloaded" do
        real_digit = @shard2.activate { Digit.create! }
        real_digit.save_shadow_record(target_shard: @shard1)
        shadow_digit = @shard1.activate { Digit.find_by("id = ?", real_digit.global_id) }
        expect(shadow_digit).to be_shadow_record
        expect(shadow_digit.shard).to eq @shard2
        expect(shadow_digit.loaded_from_shard).to eq @shard1
        shadow_digit.reload
        expect(shadow_digit).not_to be_shadow_record
        expect(shadow_digit.shard).to eq @shard2
        expect(shadow_digit.loaded_from_shard).to eq @shard2
      end

      it "works with belongs_to associations on shadow objects" do
        appendage = @user1.appendages.create!
        real_digit = @shard2.activate { Digit.create!(appendage: appendage) }
        real_digit.save_shadow_record(target_shard: @shard1)

        @shard1.activate do
          @shadow_digit = Digit.find_by("id = ?", real_digit.global_id)
          expect(@shadow_digit.shard).to eq @shard2
          expect(@shadow_digit.loaded_from_shard).to eq @shard1
          expect(@shadow_digit.id).to eq real_digit.global_id
          expect(@shadow_digit.appendage_id).to eq appendage.local_id
          expect(@shadow_digit.appendage).to eq appendage
        end

        @shard2.activate do
          expect(@shadow_digit.shard).to eq @shard2
          expect(@shadow_digit.loaded_from_shard).to eq @shard1
          expect(@shadow_digit.id).to eq real_digit.local_id
          expect(@shadow_digit.appendage_id).to eq appendage.global_id
          expect(@shadow_digit.appendage).to eq appendage
        end

        expect(@shadow_digit.shard).to eq @shard2
        expect(@shadow_digit.loaded_from_shard).to eq @shard1
        expect(@shadow_digit.id).to eq real_digit.global_id
        expect(@shadow_digit.appendage_id).to eq appendage.global_id
        expect(@shadow_digit.appendage).to eq appendage
      end

      it "works with has_many through associations with shadow objects" do
        appendage = @user1.appendages.create!
        real_digit = @shard2.activate { Digit.create!(appendage: appendage) }
        real_digit.save_shadow_record(target_shard: @shard1)

        shadow_digit = @shard1.activate { Digit.find_by("id = ?", real_digit.global_id) }
        expect(shadow_digit.shard).to eq @shard2
        expect(@user1.digits.scope.shard_value).to eq @user1
        expect(@user1.digits.find(shadow_digit.id)).to eq shadow_digit
      end

      it "sets the inverse association when preloading" do
        @user1.children.create!
        preloaded_child = User.where(id: @user1).preload(:children).first.children.first
        expect(preloaded_child.association(:parent).loaded?).to be true
        expect(preloaded_child.parent).to eq @user1
      end

      it "resolves include? correctly for a has_many :through" do
        @shard1.activate do
          child = @user1.children.create!
          @grandchild = child.children.create!
          @user1.reload
          expect(@user1.grandchildren.loaded?).to be false
          expect(@user1.grandchildren.include?(@grandchild)).to be true
        end
        @shard2.activate do
          fake = User.create!(id: @grandchild.local_id)
          @user1.reload
          expect(@user1.grandchildren.loaded?).to be false
          expect(@user1.grandchildren.include?(fake)).to be false
        end
      end

      it "shard should be changeable, and change conditions when it is changed" do
        a1 = @user1.appendages.create!
        relation = @user1.appendages.where(id: a1).shard(@shard1)
        expect(relation.shard_value).to eq @shard1
        expect(relation.shard_source_value).to eq :explicit
        expect(where_value(predicates(relation).detect { |v| v.left.name == "id" }.right)).to eq a1.local_id

        relation = @user1.appendages.where(id: a1).shard(@shard2)
        expect(relation.shard_value).to eq @shard2
        expect(relation.shard_source_value).to eq :explicit
        expect(where_value(predicates(relation).detect { |v| v.left.name == "id" }.right)).to eq a1.global_id
      end

      it "transposes predicates correctly" do
        a1 = @user1.appendages.create!
        a2 = @user2.appendages.create!

        relation = @user1.appendages.where(id: a2)
        expect(relation.shard_value).to eq @user1
        expect(where_value(predicates(relation).detect { |v| v.left.name == "id" }.right)).to eq a2.global_id

        relation = @user1.appendages.where(id: [a1, a2])
        expect(relation.shard_value).to eq @user1
        expect(where_value(predicates(relation).detect do |v|
                             v.left.name == "id"
                           end.right)).to eq [a1.local_id, a2.global_id]
      end

      it "properlies set up a cross-shard-category query" do
        @shard1.activate(MirrorUniverse) do
          mirror_user = MirrorUser.create!
          relation = mirror_user.association(:user).scope
          expect(relation.shard_value).to eq Shard.default
          klass = ::ActiveModel::Attribute
          expect(predicates(relation).first.right).to be_a(klass)
          expect(bind_values(relation)).to eq [mirror_user.global_id]
        end
      end

      it "loads singular associations from the correct shard" do
        @shard1.activate do
          @a = Appendage.create!(user_id: @user1, value: 1)
        end

        @shard2.activate do
          @d = Digit.create!(appendage_id: @a.global_id)
        end

        expect(@d.appendage).to eq @a
      end

      it "loads cross-category unsharded associations from the correct shard" do
        u = @shard1.activate { User.create! }
        r = Root.create!(user: u)
        @shard1.activate do
          expect(u.roots.to_a).to eq [r]
        end
      end

      it "loads collection associations from the correct shard" do
        @shard1.activate do
          @a = Appendage.create!(user_id: @user1, value: 1)
          @d = Digit.create!(appendage_id: @a.id)
        end

        expect(@a.digits.to_a.map(&:id)).to eq [@d.id]
      end

      describe "multishard associations" do
        it "groups has_many associations over associated_shards" do
          @shard1.activate { Appendage.create!(user_id: @user1, value: 1) }
          @shard2.activate { Appendage.create!(user: @user1, value: 2) }

          expect(@user1.appendages.to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          expect(@user1.appendages.to_a.map(&:value).sort).to eq [1, 2]
        end

        it "follow shards for has_many :through" do
          @shard1.activate { Appendage.create!(user_id: @user1).digits.create!(value: 1) }
          @shard2.activate { Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits.to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          expect(@user1.digits.to_a.map(&:value).sort).to eq [1, 2]
        end

        it "follow shards for has_many :through combined with association scope" do
          @shard1.activate { Appendage.create!(user_id: @user1).digits.create!(value: 1) }
          @shard2.activate { Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits_with_scope.to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          expect(@user1.digits_with_scope.to_a.map(&:value).sort).to eq [1, 2]
        end

        it "follows shards for has_many :through combined with where" do
          @shard1.activate { @appendage1 = Appendage.create!(user_id: @user1).digits.create!(value: 1) }
          @shard2.activate { @appendage2 = Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits.where(id: @appendage1).to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard1, @shard2]
          expect(@user1.digits.where(id: [@appendage1]).to_a.map(&:value).sort).to eq [1]
          expect(@user1.digits.where(id: [@appendage2]).to_a.map(&:value).sort).to eq [2]
          expect(@user1.digits.where(id: [@appendage1, @appendage2]).to_a.map(&:value).sort).to eq [1, 2]
        end

        it "follows shards for has_many :through combined with where when there is only a single cross-shard" do
          @shard1.activate { @appendage1 = Appendage.create!(user_id: @user1).digits.create!(value: 1) }
          @shard2.activate { @appendage2 = Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits.where(id: @appendage1).to_a.map(&:value)).to eq [1]

          @user1.reload
          @user1.associated_shards = [@shard2]
          expect(@user1.digits.where(id: @appendage1).to_a.map(&:value).sort).to eq []
          expect(@user1.digits.where(id: @appendage2).to_a.map(&:value).sort).to eq [2]
        end

        it "includes the shard in scopes created by associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate { Appendage.create!(user_id: @user1, value: 1) }
          @shard2.activate { Appendage.create!(user: @user1) }

          expect(@user1.appendages.has_no_value.to_a.size).to eq 1

          @user1.reload
          @shard2.activate { expect(@user1.appendages.has_no_value.to_a.size).to eq 1 }
        end

        it "includes the shard in scopes created by has_many :through associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate { Appendage.create!(user_id: @user1).digits.create! }
          @shard2.activate { Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits.has_no_value.count).to eq 1

          @user1.reload
          @shard2.activate { expect(@user1.digits.has_no_value.to_a.size).to eq 1 }
        end

        it "works with calculations in scopes created by associations" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate { Appendage.create!(user_id: @user1, value: 1) }
          @shard2.activate do
            Appendage.create!(user: @user1)
            @user1.appendages.create!(value: 2)
          end

          @user1.reload
          expect(@user1.appendages.has_value.sum(:value)).to eq 3

          @user1.reload
          @shard2.activate { expect(@user1.appendages.has_value.sum(:value)).to eq 3 }
        end

        it "works with calculations in scopes created by has_many :through associations" do
          @user1.associated_shards = [@shard1, @shard2]
          @shard1.activate do
            a1 = Appendage.create!(user_id: @user1)
            a1.digits.create!
            a1.digits.create!(value: 1)
          end
          @shard2.activate { Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits.has_value.sum(:value)).to eq 3
          @user1.reload
          @shard2.activate { expect(@user1.digits.has_value.sum(:value)).to eq 3 }
        end

        it "is able to explicitly set the shard and still work with named scopes" do
          @user1.associated_shards = [@shard1, @shard2]

          @shard1.activate { Appendage.create!(user_id: @user1).digits.create! }
          @shard2.activate { Appendage.create!(user_id: @user1).digits.create!(value: 2) }

          expect(@user1.digits.shard(@shard1).has_no_value.to_a.size).to eq 1
          expect(@user1.digits.shard(@shard2).has_no_value.to_a.size).to eq 0

          @user1.reload

          expect(@user1.digits.has_no_value.shard(@shard1).to_a.size).to eq 1
          expect(@user1.digits.has_no_value.shard(@shard2).to_a.size).to eq 0
        end

        describe "unsharded associations" do
          it "is able to create an unsharded new record through a collection" do
            root = @user2.roots.create!
            root.reload
            expect(root.shard).to eq Shard.default
            expect(root.user_id).to eq @user2.global_id
            expect(root.user).to eq @user2
          end

          it "fetches unsharded children" do
            @user2.roots.create!
            @user2.roots.create!
            @user2.roots.create!
            expect(@user2.reload.roots.count).to eq(3)
          end
        end

        describe "belongs_to associations" do
          it "identifies an implied shard value based on the foreign id" do
            @shard1.activate do
              @appendage = Appendage.create!(user_id: @user2.global_id)
              expect(@appendage.reload.user).to eq @user2
            end
          end

          it "translates foreign keys when replacing the record" do
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
          it "onlies run the preload query once per owner shard" do
            user3 = @shard2.activate { User.create! }
            a1 = Appendage.create!(user: @user1)
            a2 = Appendage.create!(user: @user2)
            a3 = Appendage.create!(user: user3)
            a4 = @shard2.activate { Appendage.create!(user: user3) }

            query_count = 0
            increment_query_count = lambda do |*args|
              next if args.last[:name] == "SCHEMA" # ignore metadata queries

              query_count += 1
            end

            appendages = Appendage.where(id: [a1, a2, a3, a4])
            # load this relation outside of the SQL subscription so as not to
            # increment the query count.
            appendages.load

            expected_query_count = 2 # one per shard

            ::ActiveSupport::Notifications.subscribed(increment_query_count, "sql.active_record") do
              expect do
                ::ActiveRecord::Associations::Preloader.new(records: appendages, associations: :user).call

                # pull the users off the appendages in this subscribed block to
                # show not only that they are correct, but that they are
                # preloaded (and therefore not issuing more queries)
                expect(appendages.first.user).to eq(@user1)
                expect(appendages.second.user).to eq(@user2)
                expect(appendages.third.user).to eq(user3)
                expect(appendages.fourth.user).to eq(user3)
              end.to change { query_count }.by(expected_query_count)
            end
          end

          it "invalidates the preloaded associations when a record is reloaded" do
            # this doesn't test unique switchman functionality, per se. the
            # intention is to ensure the memoization in
            # Association#associated_records_by_owner doesn't hold on to
            # associated records through a reload. (the reason it does not is
            # because the underlying Preloader instance itself gets detached,
            # so the next call to the association queries the db.)
            u = User.create!(name: "Ted")
            a_id = Appendage.create!(user: u).id
            a = Appendage.preload(:user).find(a_id)
            u.update!(name: "Theodore")
            expect(a.user.name).to eq("Ted")
            a.reload
            expect(a.user.name).to eq("Theodore")
          end

          it "preloads belongs_to associations across shards" do
            Appendage.create!(user: @user1)
            Appendage.create!(user: @user2)
            user3 = User.create!
            user3.appendages.create!

            appendages = Appendage.includes(:user).to_a
            @user1.delete

            expect(appendages.map(&:user).sort).to eq [@user1, @user2, user3].sort
          end

          it "preloads nested associations" do
            u = User.create!
            a = Appendage.create!(user: u)
            Digit.create!(appendage: a)

            u2 = User.where(id: u).preload(appendages: :digits).first
            expect(u2.association(:appendages)).to be_loaded
            expect(u2.appendages.first.association(:digits)).to be_loaded
          end

          it "preloads belongs_to :through associations across shards" do
            a1 = Appendage.create!(user: @user1)
            a1.digits.create!

            a2 = @shard1.activate { Appendage.create!(user: @user2) }
            Digit.create!(appendage: a2)

            digits = Digit.includes(:user).to_a
            @user1.delete

            expect(digits.map(&:user).sort).to eq [@user1, @user2].sort
          end

          it "preloads has_many associations across associated shards" do
            a1 = @user1.appendages.create!
            a2 = @shard2.activate { Appendage.create!(user_id: @user1) } # a2 will be in @user1's associated shards
            @shard1.activate { Appendage.create!(user_id: @user2) } # a3 is not on @user2's associated shard

            User.associated_shards_map = { @user1.global_id => [@shard1, @shard2] }

            begin
              users = User.where(id: [@user1, @user2]).includes(:appendages).to_a
              users.each { |u| expect(u.appendages.loaded?).to be true }

              u1 = users.detect { |u| u.id == @user1.id }
              u2 = users.detect { |u| u.id == @user2.id }

              a1.delete
              expect(u1.appendages.sort).to eq [a1, a2].sort
              expect(u2.appendages).to be_empty
            ensure
              User.associated_shards_map = nil
            end
          end

          it "does not find non-multishard records even if an object is associated with that shard" do
            a = Appendage.create!
            d1 = a.digits.create!
            @shard1.activate { Digit.create!(appendage_id: a) } # d2 is in associated shards, but not a's shard

            Appendage.associated_shards_map = { a.global_id => [Shard.default, @shard1] }
            begin
              appendage = Appendage.where(id: a).includes(:digits).first
              expect(appendage.digits.loaded?).to be true
              expect(appendage.digits).to eq [d1]
            ensure
              Appendage.associated_shards_map = nil
            end
          end

          it "preloads has_many :through associations across associated shards" do
            a1 = @user1.appendages.create!
            a2 = @shard2.activate { Appendage.create!(user_id: @user1) }
            a3 = @shard2.activate { Appendage.create!(user_id: @user1) }

            d1 = a1.digits.create!
            d2 = a2.digits.create! # a2 will be in @user1's associated shards
            @shard1.activate do
              Digit.create!(appendage_id: a2)
            end
            @shard1.activate { Digit.create!(appendage_id: a3) } # d4 is not on a3's shard

            a4 = @shard1.activate { Appendage.create!(user_id: @user2) }
            a5 = @user2.appendages.create!
            a6 = @user2.appendages.create!

            @shard2.activate { Digit.create!(appendage_id: a4) } # d5 is on @user2's shard but a4 is not
            @shard1.activate { Digit.create!(appendage_id: a5) } # a5 is on @user2's shard but d6 is not
            d7 = @shard2.activate { Digit.create!(appendage_id: a6) } # d7 will be in a6's associated shards

            User.associated_shards_map = { @user1.global_id => [@shard1, @shard2] }
            Appendage.associated_shards_map = { a2.global_id => [@shard1, @shard2], a6.global_id => [@shard1] }

            begin
              users = User.where(id: [@user1, @user2]).includes(:digits).to_a
              users.each { |u| expect(u.digits.loaded?).to be true }

              u1 = users.detect { |u| u.id == @user1.id }
              u2 = users.detect { |u| u.id == @user2.id }

              d1.delete

              expect(u1.digits.sort).to eq [d1, d2].sort
              expect(u2.digits).to eq [d7]
            ensure
              User.associated_shards_map = nil
              Appendage.associated_shards_map = nil
            end
          end

          it "can preload a has_many to an unsharded model" do
            u = User.create!
            r = u.roots.create!
            u2 = User.preload(:roots).where(id: u.id).take
            expect(u2.association(:roots)).to be_loaded
            expect(u2.roots.to_a).to eq [r]
          end
        end

        describe "polymorphic associations" do
          it "works normally" do
            appendage = Appendage.create!
            feature = Feature.create!(owner: appendage)

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

          it "works without statement cache" do
            allow_any_instance_of(::ActiveRecord::Associations::Association)
              .to receive(:skip_statement_cache?).and_return(true)
            f = Feature.create!(owner: @user1)
            expect(f.reload.owner).to eq @user1
          end

          it "works with multi-shard associations" do
            @shard1.activate { Feature.create!(owner: @user1, value: 1) }
            @shard2.activate { Feature.create!(owner: @user1, value: 2) }

            expect(@user1.features.to_a.map(&:value)).to eq [1]

            @user1.reload
            @user1.associated_shards = [@shard1, @shard2]
            expect(@user1.features.to_a.map(&:value).sort).to eq [1, 2]
          end
        end
      end

      it "does not break cross-shard has_one associations when autosaving" do
        face = Face.new
        face.user = @user1
        face.save!
        expect(face.user_id).to eq @user1.id # global id

        @user1.save!
        face.reload
        expect(face.user_id).to eq @user1.id # shouldn't change face's id to be @user1's local id in rails 4.2
      end

      it "does not ping-pong autosaving has_one associations" do
        face = Face.create!(user: @user1)
        expect(face).not_to receive(:save)
        @user1.save!
      end

      it "does not break cross-shard, cross-category belongs_to associations when autosaving" do
        mirror = MirrorUser.new
        @shard1.activate do
          user = User.create!
          mirror.belongs_to_user = user
          mirror.save!
          expect(mirror.belongs_to_user_id).to eq user.id
        end
      end
    end
  end
end
