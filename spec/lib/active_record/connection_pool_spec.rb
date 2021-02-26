# frozen_string_literal: true

require 'spec_helper'

module Switchman
  module ActiveRecord
    describe ConnectionPool do
      context 'with our protections' do
        self.use_transactional_tests = false
        include RSpecHelper

        it "is able to access another shard on a db server after the 'primary' shard is gone" do
          # separate connections
          server = DatabaseServer.create(Shard.default.database_server.config)
          s1 = server.shards.create!(name: 'non_existent_shard') # don't actually create any schema
          s2 = server.shards.create! # inherit's the default shard's config, which is functional
          s1.activate do
            expect { User.count }.to raise_error(::ActiveRecord::StatementInvalid)
          end
          # the config for s1 should not be the permanent default for all new
          # connections now
          s2.activate do
            expect { User.count }.not_to raise_error
          end
        end
      end

      it 'raises an error when a non-existent shard is activated' do
        Shard.new.activate do
          expect { User.count }.to raise_error(NonExistentShardError)
        end
      end

      describe 'release_connection' do
        before do
          @server = DatabaseServer.create(Shard.default.database_server.config)
          @shard = @server.shards.create!
          @pool = @shard.activate { User.connection_pool }
        end

        it 'calls flush when releasing connection' do
          expect(@pool).to receive(:flush)
          @pool.release_connection
        end
      end

      describe '#shard' do
        include RSpecHelper

        it 'is thread safe' do
          expect(User.connection_pool.shard).to eq Shard.default
          Thread.new do
            @shard1.activate!
            expect(User.connection_pool.shard).to eq @shard1
          end.join
          expect(User.connection_pool.shard).to eq Shard.default
        end
      end
    end
  end
end
