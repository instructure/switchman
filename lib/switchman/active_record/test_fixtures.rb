# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module TestFixtures
      FORBIDDEN_DB_ENVS = %i[development production].freeze

      if ::Rails.version < "7.2"
        def setup_fixtures(config = ::ActiveRecord::Base)
          super
          return unless run_in_transaction?

          # Replace the one that activerecord natively uses with a switchman-optimized one
          ::ActiveSupport::Notifications.unsubscribe(@connection_subscriber)
          # Code adapted from the code in rails proper
          @connection_subscriber =
            ::ActiveSupport::Notifications.subscribe("!connection.active_record") do |_, _, _, _, payload|
              spec_name = (payload[:connection_name] if payload.key?(:connection_name))
              shard = payload[:shard] if payload.key?(:shard)

              if spec_name && !FORBIDDEN_DB_ENVS.include?(shard)
                begin
                  connection = ::ActiveRecord::Base.connection_handler.retrieve_connection(spec_name, shard: shard)
                  connection.connect! # eagerly validate the connection
                rescue ::ActiveRecord::ConnectionNotEstablished
                  connection = nil
                end

                if connection
                  setup_shared_connection_pool
                  unless @fixture_connections.include?(connection)
                    connection.begin_transaction joinable: false, _lazy: false
                    connection.pool.lock_thread = true if lock_threads
                    @fixture_connections << connection
                  end
                end
              end
            end
        end

        def enlist_fixture_connections
          setup_shared_connection_pool

          ::ActiveRecord::Base.connection_handler.connection_pool_list(:primary).reject do |cp|
            FORBIDDEN_DB_ENVS.include?(cp.db_config.env_name.to_sym)
          end.map(&:connection)
        end
      else
        def setup_transactional_fixtures
          setup_shared_connection_pool

          # Begin transactions for connections already established
          # INST: :writing -> :primary
          @fixture_connection_pools = ::ActiveRecord::Base.connection_handler.connection_pool_list(:primary)
          # INST: filter by FORBIDDEN_DB_ENVS
          @fixture_connection_pools = @fixture_connection_pools.reject do |cp|
            FORBIDDEN_DB_ENVS.include?(cp.db_config.env_name.to_sym)
          end

          @fixture_connection_pools.each do |pool|
            pool.pin_connection!(lock_threads)
            pool.lease_connection
          end

          # When connections are established in the future, begin a transaction too
          @connection_subscriber = ::ActiveSupport::Notifications
                                   .subscribe("!connection.active_record") do |_, _, _, _, payload|
            connection_name = payload[:connection_name] if payload.key?(:connection_name)
            shard = payload[:shard] if payload.key?(:shard)

            # INST: filter by FORBIDDEN_DB_ENVS
            if connection_name && !FORBIDDEN_DB_ENVS.include?(shard)
              pool = ::ActiveRecord::Base.connection_handler.retrieve_connection_pool(connection_name, shard: shard)
              if pool
                setup_shared_connection_pool

                unless @fixture_connection_pools.include?(pool)
                  pool.pin_connection!(lock_threads)
                  pool.lease_connection
                  @fixture_connection_pools << pool
                end
              end
            end
          end
        end
      end
    end
  end
end
