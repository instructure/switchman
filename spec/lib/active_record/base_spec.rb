# frozen_string_literal: true

require 'spec_helper'

module Switchman
  module ActiveRecord
    describe Base do
      include RSpecHelper

      describe 'hash' do
        it 'works with unsharded models' do
          root = Root.create!
          expected = Root.hash ^ root.id.hash
          expect(root.hash).to eq expected
        end
      end

      describe 'find_ids_in_ranges' do
        before :all do
          @ids = []
          10.times { @ids << User.create!.id }
        end

        it 'returns ids from the table in ranges' do
          batches = []
          User.where(id: @ids).find_ids_in_ranges(batch_size: 4) do |*found_ids|
            batches << found_ids
          end
          expect(batches).to eq [[@ids[0], @ids[3]],
                                 [@ids[4], @ids[7]],
                                 [@ids[8], @ids[9]]]
        end

        it 'works with scopes' do
          user = User.create!
          user2 = User.create!
          user2.destroy
          User.active.where(id: [user, user2]).find_ids_in_ranges do |*found_ids|
            expect(found_ids).to eq [user.id, user.id]
          end
        end

        it 'accepts an option to start searching at a given id' do
          batches = []
          User.where(id: @ids).find_ids_in_ranges(batch_size: 4, start_at: @ids[3]) do |*found_ids|
            batches << found_ids
          end
          expect(batches).to eq [[@ids[3], @ids[6]], [@ids[7], @ids[9]]]
        end

        it 'accepts an option to end at a given id' do
          batches = []
          User.where(id: @ids).find_ids_in_ranges(batch_size: 4, end_at: @ids[5]) do |*found_ids|
            batches << found_ids
          end
          expect(batches).to eq [[@ids[0], @ids[3]], [@ids[4], @ids[5]]]
        end

        it 'accepts both options to start and end at given ids' do
          batches = []
          User.where(id: @ids).find_ids_in_ranges(batch_size: 4, start_at: @ids[2], end_at: @ids[7]) do |*found_ids|
            batches << found_ids
          end
          expect(batches).to eq [[@ids[2], @ids[5]], [@ids[6], @ids[7]]]
        end
      end

      describe 'to_param' do
        it 'returns nil if no id' do
          user = User.new
          expect(user.to_param).to be_nil
        end

        it 'returns the id even if not persisted' do
          user = User.new
          user.id = 1
          expect(user.to_param).to eq '1'
        end

        it 'returns local id if in the current shard' do
          user = User.create!
          expect(user.to_param).to eq user.local_id.to_s
          @shard1.activate do
            user2 = User.create!
            expect(user2.to_param).to eq user2.local_id.to_s
          end
        end

        it 'returns a short form global id if not in the current shard' do
          user = nil
          @shard1.activate do
            user = User.create!
          end
          @shard2.activate do
            expect(user.to_param).to eq "#{@shard1.id}~#{user.local_id}"
          end
        end

        it 'uses to_param in url helpers' do
          helpers = ::Rails.application.routes.url_helpers
          user = nil
          appendage = nil

          @shard1.activate do
            user = User.create!
            appendage = Appendage.create!

            expect(helpers.user_path(user)).to eq "/users/#{user.local_id}"
            expect(helpers.user_appendages_path(user)).to eq "/users/#{user.local_id}/appendages"
            expect(helpers.user_appendage_path(user,
                                               appendage)).to eq "/users/#{user.local_id}/appendages/#{appendage.local_id}"
            expect(helpers.user_test1_path(user)).to eq "/users/#{user.local_id}"
            expect(helpers.user_test2_path(user)).to eq "/users/#{user.local_id}/test2"
          end

          @shard2.activate do
            user_short_id = "#{@shard1.id}~#{user.local_id}"
            appendage_short_id = "#{@shard1.id}~#{appendage.local_id}"

            expect(helpers.user_path(user)).to eq "/users/#{user_short_id}"
            expect(helpers.user_appendages_path(user)).to eq "/users/#{user_short_id}/appendages"
            expect(helpers.user_appendage_path(user,
                                               appendage)).to eq "/users/#{user_short_id}/appendages/#{appendage_short_id}"
            expect(helpers.user_test1_path(user)).to eq "/users/#{user_short_id}"
            expect(helpers.user_test2_path(user)).to eq "/users/#{user_short_id}/test2"

            appendage2 = Appendage.create!
            expect(helpers.user_appendage_path(user,
                                               appendage2)).to eq "/users/#{user_short_id}/appendages/#{appendage2.local_id}"
          end
        end
      end

      describe 'shard=' do
        it 'adjusts foreign ids when shard is changed' do
          user = User.create!
          appendage = Appendage.new
          appendage.user_id = user.id
          appendage.shard = @shard1
          expect(appendage.attributes['user_id']).to eq user.global_id
        end
      end

      describe 'save!' do
        it 'saves cross shard objects to the right shard' do
          user = User.new
          user.name = 'a great name'
          user.shard = @shard2
          user.save!
          id = user.id
          expect(@shard2.activate { User.find(id).name }).to eq('a great name')
        end

        it 'allows saving a record on the current shard using a global id' do
          user = User.new(id: Shard.current.global_id_for(1))
          expect { user.save! }.not_to raise_error
        end

        it 'throws an error when trying to manually save! a new shadow record' do
          user = User.new(id: @shard2.global_id_for(1))
          expect { user.save! }.to raise_error(Errors::ManuallyCreatedShadowRecordError)
        end

        context 'when shadow records are writable' do
          before do
            @original_writable_value = Switchman.config[:writable_shadow_records]
            Switchman.config[:writable_shadow_records] = true
          end

          after do
            Switchman.config[:writable_shadow_records] = @original_writable_value
          end

          it 'does not throw an error when calling save! on an existing shadow record' do
            user = User.create!
            user.save_shadow_record(target_shard: @shard1)
            shadow_user = @shard1.activate { User.find_by('id = ?', user.global_id) }
            shadow_user.name = 'Fred'
            expect { shadow_user.save! }.not_to raise_error
          end
        end

        context 'when shadow records are not writable' do
          it 'throws an error when calling save! on an existing shadow record' do
            user = User.create!
            user.save_shadow_record(target_shard: @shard1)
            shadow_user = @shard1.activate { User.find_by('id = ?', user.global_id) }
            shadow_user.name = 'Fred'
            expect { shadow_user.save! }.to raise_error(::ActiveRecord::ReadOnlyRecord)
          end
        end
      end

      describe 'save' do
        it 'throws an error when trying to manually save a new shadow record' do
          user = User.new(id: @shard2.global_id_for(1))
          expect { user.save }.to raise_error(Errors::ManuallyCreatedShadowRecordError)
        end

        context 'when shadow records are writable' do
          before do
            @original_writable_value = Switchman.config[:writable_shadow_records]
            Switchman.config[:writable_shadow_records] = true
          end

          after do
            Switchman.config[:writable_shadow_records] = @original_writable_value
          end

          it 'does not throw an error when calling save on an existing shadow record' do
            user = User.create!
            user.save_shadow_record(target_shard: @shard1)
            shadow_user = @shard1.activate { User.find_by('id = ?', user.global_id) }
            shadow_user.name = 'Fred'
            expect { shadow_user.save }.not_to raise_error
          end
        end

        context 'when shadow records are not writable' do
          it 'throws an error when calling save on an existing shadow record' do
            user = User.create!
            user.save_shadow_record(target_shard: @shard1)
            shadow_user = @shard1.activate { User.find_by('id = ?', user.global_id) }
            shadow_user.name = 'Fred'
            expect { shadow_user.save }.to raise_error(::ActiveRecord::ReadOnlyRecord)
          end
        end
      end

      describe 'create!' do
        it 'throws an error when trying to manually create! a new shadow record' do
          expect { User.create!(id: @shard2.global_id_for(1)) }.to raise_error(Errors::ManuallyCreatedShadowRecordError)
        end
      end

      describe 'create' do
        it 'throws an error when trying to manually create a new shadow record' do
          expect { User.create(id: @shard2.global_id_for(1)) }.to raise_error(Errors::ManuallyCreatedShadowRecordError)
        end
      end

      describe '#loaded_from_shard' do
        it 'returns the shard for a non-shadow record' do
          user = User.create!
          expect(user.loaded_from_shard).to eq Shard.default
        end

        it 'returns the shard the record was actually loaded from for a shadow record' do
          user = User.create!
          user.save_shadow_record(target_shard: @shard2)
          shadow_user = @shard2.activate { User.find_by('id = ?', user.global_id) }
          expect(shadow_user.loaded_from_shard).to eq @shard2
        end

        it 'uses the shard as a fallback value when the ivar is not set' do
          user = User.create!
          user.instance_variable_set(:@loaded_from_shard, nil)
          @shard2.activate do
            expect(user.loaded_from_shard).to eq user.shard
          end
        end
      end

      # Note this also tests `save_shadow_record` through the after hook on user.rb
      describe '#shadow_record?' do
        it 'correctly identifies shadow records' do
          user = User.new
          user.name = 'a great name'
          user.shard = @shard2
          user.save!

          shadow_user = User.where('id=?', user.id).first
          expect(shadow_user.name).to eq(user.name)
          expect(user.shadow_record?).to be(false)
          expect(shadow_user.shadow_record?).to be(true)

          @shard2.activate do
            expect(user.shadow_record?).to be(false)
            expect(shadow_user.shadow_record?).to be(true)
          end
        end

        it 'loads shadow records as readonly' do
          user = User.new
          user.name = 'a great name'
          user.shard = @shard2
          user.save!

          shadow_user = User.where('id=?', user.id).first
          expect(shadow_user.name).to eq(user.name)
          expect(shadow_user.readonly?).to be(true)
        end
      end

      describe '#destroy_shadow_records' do
        before do
          @user = User.create!(name: 'John Doe')
          @user.save_shadow_record(target_shard: @shard1)
        end

        it 'can be passed a single target shard' do
          expect { @user.destroy_shadow_records(target_shards: @shard1) }.to change {
            @shard1.activate { User.where('id = ?', @user.global_id) }.count
          }.from(1).to(0)
        end

        it 'does not delete shadow records on shards not included' do
          @user.save_shadow_record(target_shard: @shard2)
          expect { @user.destroy_shadow_records(target_shards: @shard1) }.not_to change {
            @shard2.activate { User.where('id = ?', @user.global_id) }.count
          }.from(1)
        end

        it 'can be passed a collection of target shards' do
          @user.save_shadow_record(target_shard: @shard2)
          expect { @user.destroy_shadow_records(target_shards: [@shard1, @shard2]) }.to change {
            [@shard1, @shard2].reduce(0) do |count, shard|
              count + shard.activate { User.where('id = ?', @user.global_id).count }
            end
          }.from(2).to(0)
        end

        it 'does not delete the root record (even when passed the root record shard)' do
          expect { @user.destroy_shadow_records(target_shards: @user.shard) }.not_to change {
            User.where(id: @user.id).count
          }.from(1)
        end
      end

      describe '.unscoped' do
        it "doesn't capture the shard permanently (block form)" do
          @shard1.activate do
            User.unscoped do
              Shard.default.activate do
                expect(User.all.shard_value).to eq Shard.default
              end
            end
          end
        end

        it "doesn't capture the shard permanently" do
          @shard1.activate do
            scope = User.unscoped
            scope.scoping do
              Shard.default.activate do
                expect(User.all.shard_value).to eq Shard.default
              end
            end
          end
        end
      end

      describe '#id_for_database' do
        it 'transposes correctly' do
          user = @shard1.activate { User.create! }
          expect(user.id_for_database).to eq user.global_id
          @shard1.activate do
            expect(user.id_for_database).to eq user.local_id
          end
        end
      end

      it 'does not change scopes when saving STI objects' do
        a1 = Appendage.create!
        a2 = Appendage.create!(type: 'Arm')

        a2.should_test_scoping = true
        a2.save!
        expect(a2.all_appendages).to include(a1)
      end

      it 'allows activating other shards for the model in callbacks' do
        u = nil
        @shard2.activate do
          u = User.create!(name: 'name1')
        end
        expect(User.where('id=?', u.global_id).first.name).to eq 'name1'
        u.update!(name: 'name2')
        expect(User.where('id=?', u.global_id).first.name).to eq 'name2'
      end
    end
  end
end
