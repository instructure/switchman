# frozen_string_literal: true

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
        it "should activate multiple shards if necessary" do
          expect(User.where(:id => [@user1.id, @user2.id]).sort_by(&:id)).to eq [@user1, @user2].sort_by(&:id)
        end

        it 'raises an error if you have a sort order on a multi-shard query' do
          expect { User.where(id: [@user1.id, @user2.id]).order(::Arel.sql('id')).to_a }.to raise_error(OrderOnMultiShardQuery)
        end

        it "doesn't whine about sort order when doing unordered operations" do
          expect(User.where(id: [@user1.id, @user2.id]).update_all(name: 'a')).to eq 2
        end

        it 'implements cross-shard limit' do
          expect(User.where(id: [@user1.id, @user2.id]).limit(1).to_a).to eq [@user1]
        end

        it 'implement cross-shard limit on non-boundary' do
          @user3 = @shard1.activate { User.create! }
          expect(User.where(id: [@user1.id, @user2.id, @user3.id]).limit(2).to_a.length).to eq 2
        end

        context 'with cross-shard sorting' do
          it 'handles a basic case' do
            expect(User.where(id: [@user1.id, @user2.id]).order(id: :desc)).to eq [@user1, @user2].sort_by(&:id).reverse
          end

          it 'sorts nulls last' do
            @user3 = User.create!
            expect(User.where(id: [@user1.id, @user2.id, @user3.id]).order(:name).to_a).to eq [@user1, @user2, @user3]
          end

          it 'sorts the full set, even with a limit' do
            @user3 = @shard1.activate { User.create!(name: 'a') }
            expect(User.where(id: [@user1.id, @user2.id, @user3.id]).order(:name).limit(2).to_a).to eq [@user3, @user1]
          end

          it 'sorts a pluck' do
            expect(User.where(id: [@user1.id, @user2.id]).order(id: :desc).pluck(:id)).to eq [@user1, @user2].map(&:id).sort.reverse
          end
        end
      end

      describe "#update_all" do
        it "should activate the correct shard for the query" do
          User.shard(@shard1).where(:id => @user2.local_id).update_all(:name => 'a')
          expect(@user1.reload.name).to eq 'user1'
          expect(@user2.reload.name).to eq 'user2'
          User.shard(@shard1).where(:id => @user2.global_id).update_all(:name => 'a')
          expect(@user2.reload.name).to eq 'a'
        end

        it "should activate multiple shards if necessary" do
          expect(User.where(:id => [@user1.id, @user2.id]).update_all(:name => 'a')).to eq 2
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

      describe "#clone" do
        it "sets the shard_value that was previously nil" do
          scope = User.all
          scope.shard_value = nil
          scope = scope.clone
          expect(scope.shard_value).to eq Shard.current
        end
      end

      describe "#to_sql" do
        it "activates the primary shard for qualified name purposes" do
          sql = User.shard(@shard1).to_sql
          expect(sql).to be_include(@shard1.name)
          expect(sql).not_to be_include(Shard.default.name)
        end
      end
    end
  end
end
