# frozen_string_literal: true

require "spec_helper"

module Switchman
  describe GuardRail do
    include RSpecHelper

    before do
      #!!! trick GuardRail in to actually switching envs
      allow(::Rails.env).to receive(:test?).and_return(false)

      # be sure to test bugs where the current env isn't yet included in this hash
      ::GuardRail.connection_handlers.clear
    end

    it "should capture the correct current_pool" do
      # use @shard2 cause it has its own DatabaseServer
      @shard2.activate do
        @current_pool = ::GuardRail.activate(:secondary) { ::ActiveRecord::Base.connection_pool.current_pool }
      end
      ::GuardRail.activate(:secondary) do
        expect(::ActiveRecord::Base.connection_pool.default_pool).not_to eq @current_pool
      end
    end

    it "should correctly set up pools for sharding categories" do
      models = ::ActiveRecord::Base.connection_handler.send(:owner_to_pool)
      default_pools = {}
      models.each_pair { |k, v| default_pools[k] = v.current_pool }
      ::GuardRail.activate(:secondary_that_no_one_else_uses) do
        models = ::ActiveRecord::Base.connection_handler.send(:owner_to_pool)
        pools = {}
        models.each_pair { |k, v| pools[k] = v.current_pool }
        expect(default_pools.keys.sort).to eq pools.keys.sort
        default_pools.keys.each do |model|
          expect(default_pools[model]).not_to eq pools[model]
        end
      end
    end

    it "should connect to the first working secondary" do
      # have to unstub long enough to create this
      allow(::Rails.env).to receive(:test?).and_call_original
      ds = DatabaseServer.create(Shard.default.database_server.config.merge(
        :secondary => [{ host: 'some.postgres.server' }, nil]))
      allow(::Rails.env).to receive(:test?).and_return(false)
      ds.guard!
      s = ds.shards.create!
      s.activate do
        User.connection
        expect(User.connection_pool.spec.config[:host]).not_to eq 'some.postgres.server'
      end
    end

    it "should unguard the appropriate connection when the scope changes connections" do
      begin
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
    end

    it "should unguard for FOR UPDATE queries" do
      begin
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
    end

    it "should not get confused about a single guarded server" do
      begin
        Shard.default.database_server.guard!
        # have to unstub long enough to create this
        allow(::Rails.env).to receive(:test?).and_call_original
        ds = DatabaseServer.create(adapter: 'postgresql', host: 'notguarded', secondary: { host: 'guarded' })
        allow(::Rails.env).to receive(:test?).and_return(false)
        s = ds.shards.create!
        s.activate do
          expect(User.connection_pool.spec.config[:host]).to eq 'notguarded'
        end
      ensure
        Shard.default.database_server.unguard!
      end
    end

    it "should track all activated environments" do
      ::GuardRail.activate(:secondary) {}
      ::GuardRail.activate(:custom) {}
      expected = Set.new([:primary, :secondary, :custom])
      expect(::GuardRail.activated_environments & expected).to eq expected
    end

    context "non-transactional" do
      self.use_transactional_tests = ::ActiveRecord::Base.connection.supports_ddl_transactions?

      it "should really disconnect all envs" do
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
        ::ActiveRecord::Base.connection_pool.current_pool.instance_variable_get(:@thread_cached_conns).size
      end

      it "should really return active connections to the pool in all envs" do
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

      it "should not establish connections when switching environments" do
        ::ActiveRecord::Base.clear_all_connections!
        expect(::ActiveRecord::Base.connection_pool).not_to be_connected
        ::GuardRail.activate(:secondary) {}
        expect(::ActiveRecord::Base.connection_pool).not_to be_connected
      end
    end
  end
end
