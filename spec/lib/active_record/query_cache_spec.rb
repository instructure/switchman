# frozen_string_literal: true

require "spec_helper"

module Switchman
  module ActiveRecord
    describe QueryCache do
      include RSpecHelper

      after do
        ::ActiveRecord::Base.connection_pool.disable_query_cache!
      end

      it "Works when doing updates with a shard activated" do
        ::ActiveRecord::Base.connection_pool.enable_query_cache!
        @shard1.activate do
          @user1 = User.create!
        end
        @shard2.activate do
          @user2 = User.create!
        end

        root = Root.create!(user: @user1)
        users = @shard1.activate do
          root.update(user: @user2)
          User.all.includes(:roots).to_a
        end
        expect(users[0].roots).to eq([])
      end

      it "isolates queries to multiple shards on the same server" do
        expect(::ActiveRecord::Base.connection_pool.query_cache_enabled).to be false
        ::ActiveRecord::Base.connection_pool.enable_query_cache!

        @shard1.activate do
          expect(User.connection.query_cache_enabled).to be true
          User.create!
          User.create!
        end
        Shard.default.activate do
          expect(User.connection.query_cache_enabled).to be true
          User.create!
        end
        expect(@shard1.activate { User.all.to_a }).not_to eq(Shard.default.activate { User.all.to_a })
        @shard1.activate { expect(User.connection).not_to receive(:select) }
        expect(@shard1.activate { User.all.to_a }).not_to eq(Shard.default.activate { User.all.to_a })
      end

      it "doesn't break logging with binds" do
        expect(::Rails.logger).not_to receive(:error)
        User.connection.cache do
          User.where(id: 1).take
          User.where(id: 1).take
        end
      end

      it "clears the query cache on write" do
        u = User.create
        User.cache do
          User.first

          count = 0
          allow(User.connection).to receive(:internal_exec_query).and_wrap_original do |original, *args, **kwargs|
            count += 1
            original.call(*args, **kwargs)
          end
          User.first
          expect(count).to eq 0

          u2 = User.new
          u2.id = u.id
          expect { u2.save! }.to raise_error(::ActiveRecord::RecordNotUnique)
          count = 0
          User.first
          expect(count).to eq 1
        end
      end
    end
  end
end
