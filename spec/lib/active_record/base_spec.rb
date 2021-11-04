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
    end
  end
end
