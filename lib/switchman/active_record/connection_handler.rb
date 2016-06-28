require 'switchman/connection_pool_proxy'

module Switchman
  module ActiveRecord
    module ConnectionHandler
      def self.make_sharing_automagic(config)
        key = config[:adapter] == 'postgresql' ? :schema_search_path : :database

        # only load the shard name from the db if we have to
        if config[key] || !config[:shard_name]
          # we may not be able to connect to this shard yet, cause it might be an empty database server
          shard_name = Shard.current.name rescue nil
          return unless shard_name

          config[:shard_name] ||= shard_name
        end

        if !config[key] || config[key] == shard_name
          # this may truncate the schema_search_path if it was not specified in database.yml
          # but that's what our old behavior was anyway, so I guess it's okay
          config[key] = '%{shard_name}'
        end
      end

      def establish_connection(*args)
        pool = super
        owner, spec = ::Rails.version < '5' ? args : [nil, args.first]

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

          # automatically change config to allow for sharing connections with simple config
          ConnectionHandler.make_sharing_automagic(spec.config)
          ConnectionHandler.make_sharing_automagic(Shard.default.database_server.config)

          ::ActiveRecord::Base.configurations[::Rails.env] = spec.instance_variable_get(:@config).stringify_keys
        end

        @shard_connection_pools ||= { [:master, Shard.default.database_server.shareable? ? ::Rails.env : Shard.default] => pool}

        category = ::Rails.version < '5' ? owner.shard_category : pool.spec.name.to_sym
        proxy = ConnectionPoolProxy.new(category,
                                        pool,
                                        @shard_connection_pools)
        if ::Rails.version < '5'
          owner_to_pool[owner.name] = proxy
          class_to_pool.clear
        else
          owner_to_pool[pool.spec.name] = proxy
        end

        if first_time
          if Shard.default.database_server.config[:prefer_slave]
            Shard.default.database_server.shackle!
          end

          if Shard.default.is_a?(DefaultShard) && Shard.default.database_server.config[:slave]
            Shard.default.database_server.shackle!
            Shard.default(true)
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
              shard = server.shards.where(:name => nil).first
              shard ||= Shard.new(:database_server => server)
              shard.activate do
                ConnectionHandler.make_sharing_automagic(server.config)
                ConnectionHandler.make_sharing_automagic(proxy.current_pool.spec.config)
              end
            end
          end
          # we may have established some connections above trying to infer the shard's name.
          # close them, so that someone that doesn't expect them doesn't try to fork
          # without closing them
          self.clear_all_connections!
        end

        proxy
      end

      if ::Rails.version < '5'
        def remove_connection(model)
          uninitialize_ar(model) if owner_to_pool[model.name].is_a?(ConnectionPoolProxy)
          result = super
          initialize_categories
          result
        end

        def pool_for(owner)
          # copypasted from AR#ConnectionHandler other than proxy handling

          owner_to_pool.fetch(owner.name) {
            if ancestor_pool = pool_from_any_process_for(owner)
              # A connection was established in an ancestor process that must have
              # subsequently forked. We can't reuse the connection, but we can copy
              # the specification and establish a new connection with it.
              if ancestor_pool.is_a?(ConnectionPoolProxy)
                establish_connection owner, ancestor_pool.default_pool.spec
              else
                establish_connection owner, ancestor_pool.spec
              end
            else
              owner_to_pool[owner.name] = nil
            end
          }
        end

        def retrieve_connection_pool(klass)
          class_to_pool[klass.name] ||= begin
            original_klass = klass
            until pool = pool_for(klass)
              klass = klass.superclass
              break unless klass <= Base
            end

            if pool.is_a?(ConnectionPoolProxy) && pool.category != original_klass.shard_category
              default_pool = pool.default_pool
              pool = nil
              class_to_pool.each_value { |p| pool = p if p.is_a?(ConnectionPoolProxy) &&
                  p.category == original_klass.shard_category &&
                  p.default_pool == default_pool }
              pool ||= ConnectionPoolProxy.new(original_klass.shard_category, default_pool, @shard_connection_pools)
            end

            class_to_pool[original_klass.name] = pool
          end
        end
      else
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
              pool = nil
              if ancestor_pool.is_a?(ConnectionPoolProxy)
                pool = establish_connection ancestor_pool.default_pool.spec
              else
                pool = establish_connection ancestor_pool.spec
              end
              pool.instance_variable_set(:@schema_cache, ancestor_pool.schema_cache) if ancestor_pool.schema_cache
              pool
            elsif spec_name != "primary"
              primary_pool = retrieve_connection_pool("primary")
              if primary_pool.is_a?(ConnectionPoolProxy)
                ConnectionPoolProxy.new(spec_name.to_sym, primary_pool.default_pool, @shard_connection_pools)
              else
                primary_pool
              end
            else
              owner_to_pool[spec_name] = nil
            end
          end
        end
      end

      def clear_idle_connections!(since_when)
        # TODO in rails 4.2+ s/connection_pools.values/connection_pool_list/
        connection_pools.values.each{ |pool| pool.clear_idle_connections!(since_when) }
      end

      def switchman_connection_pool_proxies
        (::Rails.version < '5' ? class_to_pool : owner_to_pool).values.uniq.select{|p| p.is_a?(ConnectionPoolProxy)}
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
