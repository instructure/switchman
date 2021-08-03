# frozen_string_literal: true

require 'spec_helper'

module Switchman
  describe GuardRail do
    include RSpecHelper

    it 'connects to the first working secondary' do
      ds = DatabaseServer.create(Shard.default.database_server.config.merge(
                                   secondary: [{ host: 'some.postgres.server' }, nil]
                                 ))
      ds.guard!
      s = ds.shards.create!
      s.activate do
        User.connection
        expect(User.connection_pool.db_config.configuration_hash[:host]).not_to eq 'some.postgres.server'
      end
    end

    it 'unguards the appropriate connection when the scope changes connections' do
      Shard.default.database_server.guard!
      @shard2.activate do
        expect(Shard.default.database_server.guard_rail_environment).to eq :secondary
        expect(::GuardRail.environment).to eq :primary

        expect(Shard.default.database_server).to receive(:unguard).once
        User.shard(Shard.default).update_all(updated_at: nil)
      end
    ensure
      Shard.default.database_server.unguard!
    end

    it 'unguards for FOR UPDATE queries' do
      Shard.default.database_server.guard!
      expect(Shard.default.database_server.guard_rail_environment).to eq :secondary

      u = User.create!
      expect(Shard.default.database_server).to receive(:unguard).once.and_return([])
      User.lock.first
      expect(Shard.default.database_server).to receive(:unguard).once.and_return([])
      expect { u.lock! }.to raise_error(::ActiveRecord::RecordNotFound)
    ensure
      Shard.default.database_server.unguard!
    end

    it 'does not get confused about a single guarded server' do
      Shard.default.database_server.guard!
      # have to unstub long enough to create this
      allow(::Rails.env).to receive(:test?).and_call_original
      ds = DatabaseServer.create(adapter: 'postgresql', host: 'notguarded', secondary: { host: 'guarded' })
      allow(::Rails.env).to receive(:test?).and_return(false)
      s = ds.shards.create!
      s.activate do
        expect(User.connection_pool.db_config.configuration_hash[:host]).to eq 'notguarded'
      end
    ensure
      Shard.default.database_server.unguard!
    end

    it 'unguards update_record queries when a different shard is active' do
      Shard.default.database_server.guard!
      expect(Shard.default.database_server.guard_rail_environment).to eq :secondary
      expect(@shard2.database_server.guard_rail_environment).to eq :primary

      u = User.create!
      @shard2.activate do
        expect(Shard.default.database_server).to receive(:unguard).once.and_return([])
        u.update_columns(name: 'bob')
      end
    ensure
      Shard.default.database_server.unguard!
    end

    it 'tracks all activated environments' do
      ::GuardRail.activate(:secondary) {}
      ::GuardRail.activate(:custom) {}
      expect(DatabaseServer.all_roles).to include(*%i[primary secondary custom])
    end

    context 'without transaction' do
      self.use_transactional_tests = ::ActiveRecord::Base.connection.supports_ddl_transactions?

      it 'really disconnect all envs' do
        ::ActiveRecord::Base.connection
        expect(::ActiveRecord::Base.connection_pool).to be_connected
        @shard1.activate do
          ::ActiveRecord::Base.connection
          expect(::ActiveRecord::Base.connection_pool).to be_connected
        end
        @shard2.activate do
          ::ActiveRecord::Base.connection
          expect(::ActiveRecord::Base.connection_pool).to be_connected
        end

        ::GuardRail.activate(:secondary) do
          ::ActiveRecord::Base.connection
          expect(::ActiveRecord::Base.connection_pool).to be_connected
          @shard1.activate do
            ::ActiveRecord::Base.connection
            expect(::ActiveRecord::Base.connection_pool).to be_connected
          end
          @shard2.activate do
            ::ActiveRecord::Base.connection
            expect(::ActiveRecord::Base.connection_pool).to be_connected
          end
        end

        ::ActiveRecord::Base.clear_all_connections!
        expect(::ActiveRecord::Base.connection_pool).not_to be_connected
        @shard1.activate do
          expect(::ActiveRecord::Base.connection_pool).not_to be_connected
        end
        @shard2.activate do
          expect(::ActiveRecord::Base.connection_pool).not_to be_connected
        end
        ::GuardRail.activate(:secondary) do
          expect(::ActiveRecord::Base.connection_pool).not_to be_connected
          @shard1.activate do
            expect(::ActiveRecord::Base.connection_pool).not_to be_connected
          end
          @shard2.activate do
            expect(::ActiveRecord::Base.connection_pool).not_to be_connected
          end
        end
      end

      def actual_connection_count
        ::ActiveRecord::Base.connection_pool.instance_variable_get(:@thread_cached_conns).size
      end

      it 'reallies return active connections to the pool in all envs' do
        ::ActiveRecord::Base.connection
        expect(actual_connection_count).not_to eq 0
        @shard1.activate do
          ::ActiveRecord::Base.connection
          expect(actual_connection_count).not_to eq 0
        end
        @shard2.activate do
          ::ActiveRecord::Base.connection
          expect(actual_connection_count).not_to eq 0
        end

        ::GuardRail.activate(:secondary) do
          ::ActiveRecord::Base.connection
          expect(actual_connection_count).not_to eq 0
          @shard1.activate do
            ::ActiveRecord::Base.connection
            expect(actual_connection_count).not_to eq 0
          end
          @shard2.activate do
            ::ActiveRecord::Base.connection
            expect(actual_connection_count).not_to eq 0
          end
        end

        ::ActiveRecord::Base.clear_active_connections!
        expect(actual_connection_count).to eq 0
        @shard1.activate do
          expect(actual_connection_count).to eq 0
        end
        @shard2.activate do
          expect(actual_connection_count).to eq 0
        end
        ::GuardRail.activate(:secondary) do
          expect(actual_connection_count).to eq 0
          @shard1.activate do
            expect(actual_connection_count).to eq 0
          end
          @shard2.activate do
            expect(actual_connection_count).to eq 0
          end
        end
      end

      it 'does not establish connections when switching environments' do
        ::ActiveRecord::Base.clear_all_connections!
        expect(::ActiveRecord::Base.connection_pool).not_to be_connected
        ::GuardRail.activate(:secondary) {}
        expect(::ActiveRecord::Base.connection_pool).not_to be_connected
      end
    end
  end
end
