require 'switchman/connection_pool_proxy'

module Switchman
  module ActiveRecord
    module ConnectionHandler
      def self.make_sharing_automagic(config, shard = Shard.current)
        # only load the shard name from the db if we have to
        if !config[:shard_name]
          # we may not be able to connect to this shard yet, cause it might be an empty database server
          shard = shard.call if shard.is_a?(Proc)
          shard_name = shard.name rescue nil
          return unless shard_name

          config[:shard_name] ||= shard_name
        end
      end

      def establish_connection(spec)
        # Just skip establishing a sharded connection if sharding isn't loaded; we'll do it again later
        # This only can happen when loading ActiveRecord::Base; after everything is loaded Shard will
        # be defined and this will actually establish a connection
        return unless defined?(Shard)
        pool = super

        # this is the first place that the adapter would have been required; but now we
        # need this addition ASAP since it will be called when loading the default shard below
        if defined?(::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
          require "switchman/active_record/postgresql_adapter"
          ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(ActiveRecord::PostgreSQLAdapter)
        end

        first_time = !Shard.instance_variable_get(:@default)
        if first_time
          # Have to cache the default shard before we insert sharding, otherwise the first access
          # to sharding will recurse onto itself trying to access column information
          Shard.default

          config = pool.spec.config
          # automatically change config to allow for sharing connections with simple config
          ConnectionHandler.make_sharing_automagic(config)
          ConnectionHandler.make_sharing_automagic(Shard.default.database_server.config)

          if ::Rails.version < '6.0'
            ::ActiveRecord::Base.configurations[::Rails.env] = config.stringify_keys
          else
            # Adopted from the deprecated code that currently lives in rails proper
            remaining_configs = ::ActiveRecord::Base.configurations.configurations.reject { |db_config| db_config.env_name == ::Rails.env }
            new_config = ::ActiveRecord::DatabaseConfigurations.new(::Rails.env => config.stringify_keys).configurations
            new_configs = remaining_configs + new_config

            ::ActiveRecord::Base.configurations = new_configs
          end
        else
          # this is probably wrong now
          Shard.default.remove_instance_variable(:@name) if Shard.default.instance_variable_defined?(:@name)
        end

        @shard_connection_pools ||= { [:master, Shard.default.database_server.shareable? ? ::Rails.env : Shard.default] => pool}

        category = pool.spec.name.to_sym
        proxy = ConnectionPoolProxy.new(category,
                                        pool,
                                        @shard_connection_pools)
        owner_to_pool[pool.spec.name] = proxy

        if first_time
          if Shard.default.database_server.config[:prefer_slave]
            Shard.default.database_server.shackle!
          end

          if Shard.default.is_a?(DefaultShard) && Shard.default.database_server.config[:slave]
            Shard.default.database_server.shackle!
            Shard.default(reload: true)
          end
        end

        # reload the default shard if we just got a new connection
        # to where the Shards table is
        # DON'T do it if we're not the current connection handler - that means
        # we're in the middle of switching environments, and we don't want to
        # establish a connection with incorrect settings
        if [:primary, :unsharded].include?(category) && self == ::ActiveRecord::Base.connection_handler && !first_time
          Shard.default(reload: true, with_fallback: true)
          proxy.disconnect!
        end

        if first_time
          # do the change for other database servers, now that we can switch shards
          if Shard.default.is_a?(Shard)
            DatabaseServer.all.each do |server|
              next if server == Shard.default.database_server

              shard = nil
              shard_proc = -> do
                shard ||= server.shards.where(:name => nil).first
                shard ||= Shard.new(:database_server => server)
                shard
              end
              ConnectionHandler.make_sharing_automagic(server.config, shard_proc)
              ConnectionHandler.make_sharing_automagic(proxy.current_pool.spec.config, shard_proc)
            end
          end
          # we may have established some connections above trying to infer the shard's name.
          # close them, so that someone that doesn't expect them doesn't try to fork
          # without closing them
          self.clear_all_connections!
        end

        proxy
      end

      def remove_connection(spec_name)
        pool = owner_to_pool[spec_name]
        owner_to_pool[spec_name] = pool.default_pool if pool.is_a?(ConnectionPoolProxy)
        super
      end

      def retrieve_connection_pool(spec_name)
        owner_to_pool.fetch(spec_name) do
          if ancestor_pool = pool_from_any_process_for(spec_name)
            # A connection was established in an ancestor process that must have
            # subsequently forked. We can't reuse the connection, but we can copy
            # the specification and establish a new connection with it.
            spec = if ancestor_pool.is_a?(ConnectionPoolProxy)
              ancestor_pool.default_pool.spec
            else
              ancestor_pool.spec
            end
            pool = establish_connection(spec.to_hash)
            pool.instance_variable_set(:@schema_cache, ancestor_pool.schema_cache) if ancestor_pool.schema_cache
            pool
          elsif spec_name != "primary"
            primary_pool = retrieve_connection_pool("primary")
            if primary_pool.is_a?(ConnectionPoolProxy)
              pool = ConnectionPoolProxy.new(spec_name.to_sym, primary_pool.default_pool, @shard_connection_pools)
              pool.schema_cache.copy_values(primary_pool.schema_cache)
              pool
            else
              primary_pool
            end
          else
            owner_to_pool[spec_name] = nil
          end
        end
      end

      def clear_idle_connections!(since_when)
        connection_pool_list.each{ |pool| pool.clear_idle_connections!(since_when) }
      end

      def switchman_connection_pool_proxies
        owner_to_pool.values.uniq.select{|p| p.is_a?(ConnectionPoolProxy)}
      end

      private

      # semi-private
      public
      def uninitialize_ar(model = ::ActiveRecord::Base)
        # take the proxies out
        pool = owner_to_pool[model.name]
        owner_to_pool[model.name] = pool.default_pool if pool
      end

      def initialize_categories(model = ::ActiveRecord::Base)
        class_to_pool.clear
      end
    end
  end
end
