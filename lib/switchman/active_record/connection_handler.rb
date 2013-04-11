require_dependency 'switchman/connection_pool_proxy'
require_dependency 'switchman/shard'

module Switchman
  module ActiveRecord
    module ConnectionHandler
      def self.make_sharing_automagic(config, shard)
        key = config[:adapter] == 'postgresql' ? :schema_search_path : :database

        # we may not be able to connect to this shard yet, cause it might be an empty database server
        shard_name = shard.name rescue nil
        return unless shard_name

        config[:shard_name] ||= shard_name
        if !config[key] || config[key] == shard_name
          # this may truncate the schema_search_path if it was not specified in database.yml
          # but that's what our old behavior was anyway, so I guess it's okay
          config[key] = '%{shard_name}'
        end
      end

      def self.included(klass)
        klass.alias_method_chain(:establish_connection, :sharding)
        klass.alias_method_chain(:remove_connection, :sharding)
      end

      def establish_connection_with_sharding(name, spec)
        establish_connection_without_sharding(name, spec)

        # this is the first place that the adapter would have been required; but now we
        # need this addition ASAP since it will be called when loading the default shard below
        if defined?(::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
          require "switchman/active_record/postgresql_adapter"
          ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.send(:include, ActiveRecord::PostgreSQLAdapter)
        end

        model = name.constantize
        pool = connection_pools[spec]

        first_time = !Shard.instance_variable_get(:@default)
        if first_time
          # Have to cache the default shard before we insert sharding, otherwise the first access
          # to sharding will recurse onto itself trying to access column information
          Shard.default

          # automatically change config to allow for sharing connections with simple config
          ConnectionHandler.make_sharing_automagic(spec.config, Shard.default)
          ConnectionHandler.make_sharing_automagic(Shard.default.database_server.config, Shard.default)
        end
        @shard_connection_pools ||= { Shard.default.database_server.id => pool }

        proxy = ConnectionPoolProxy.new(model.shard_category,
                                        pool,
                                        @shard_connection_pools)
        connection_pools[spec] = proxy

        initialize_categories(model)
        @class_to_pool[name] = proxy

        # reload the default shard if we just got a new connection
        # to where the Shards table is
        # DON'T do it if we're not the current connection handler - that means
        # we're in the middle of switching environments, and we don't want to
        # establish a connection with incorrect settings
        if (model == ::ActiveRecord::Base || model == Shard) && self == ::ActiveRecord::Base.connection_handler && !first_time
          Shard.default(true) unless first_time
          proxy.disconnect!
        end

        if first_time
          # do the change for other database servers, now that we can switch shards
          if Shard.default.is_a?(Shard)
            DatabaseServer.all.each do |server|
              next if server == Shard.default.database_server
              shard = server.shards.where(:name => nil).first
              shard ||= Shard.new(:database_server => server)
              ConnectionHandler.make_sharing_automagic(server.config, shard)
            end
          end
          # we may have established some connections above trying to infer the shard's name.
          # close them, so that someone that doesn't expect them doesn't try to fork
          # without closing them
          self.clear_all_connections!
        end
      end

      def remove_connection_with_sharding(model)
        uninitialize_ar(model) if @class_to_pool[model.name].is_a?(ConnectionPoolProxy)
        remove_connection_without_sharding(model)
      end

      private

      def uninitialize_ar(model = ::ActiveRecord::Base)
        # take the proxies out
        @class_to_pool.each_key do |model_name|
          pool_model = model_name.constantize
          # only de-proxify models that inherit from what we're uninitializing
          next unless pool_model == model || pool_model < model
          proxy = @class_to_pool[model_name]
          next unless proxy.is_a?(ConnectionPoolProxy)

          # make sure we're switched back to the default shard for the
          # connection that will remain
          if proxy.connected?
            Shard.default.activate { proxy.connection }
          end
          connection_pools[proxy.spec] = proxy.default_pool
          @class_to_pool[model_name] = proxy.default_pool
        end

        # prune dups that were created for implementing shard categories
        @class_to_pool.each_key do |model_name|
          next if model_name == ::ActiveRecord::Base.name
          pool_model = model_name.constantize
          @class_to_pool.delete(model_name) if retrieve_connection_pool(pool_model.superclass) == @class_to_pool[model_name]
        end
      end

      # semi-private
      public
      def initialize_categories(model = ::ActiveRecord::Base)
        # now set up pools for models that inherit from this model, but with a different
        # sharding category
        Shard.const_get(:CATEGORIES).each do |category, models|
          next if category == :default
          next if category == model.shard_category

          this_proxy = nil
          Array(models).each do |category_model|
            category_model = category_model.constantize if category_model.is_a? String
            next unless category_model < model

            # don't replace existing connections
            next if @class_to_pool[category_model.name]

            default_pool = retrieve_connection_pool(model)
            default_pool = default_pool.default_pool if default_pool.is_a?(ConnectionPoolProxy)
            # look for an existing compatible proxy for this category
            this_proxy ||= ConnectionPoolProxy.new(category_model.shard_category, default_pool, @shard_connection_pools)
            @class_to_pool[category_model.name] = this_proxy
          end
        end
      end
    end
  end
end
