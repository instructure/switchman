module Switchman
  class Engine < ::Rails::Engine
    isolate_namespace Switchman

    def self.lookup_stores(cache_store_config)
      result = {}
      cache_store_config.each do |key, value|
        next if value.is_a?(String)
        result[key] = ::ActiveSupport::Cache.lookup_store(value)
      end

      cache_store_config.each do |key, value|
        next unless value.is_a?(String)
        result[key] = result[value]
      end
      result
    end

    initializer 'switchman.initialize_cache', :before => 'initialize_cache' do
      require "switchman/active_support/cache"
      ::ActiveSupport::Cache::Store.send(:include, ActiveSupport::Cache::Store)

      # if we haven't already setup our cache map out-of-band, set it up from
      # config.cache_store now. behaves similarly to Rails' default
      # initialize_cache initializer, but for each value in the map, rather
      # than just Rails.cache. if config.cache_store is a flat value, uses it
      # to fill just the Rails.env entry in the cache map.
      unless Switchman.config[:cache_map].present?
        cache_store_config = ::Rails.configuration.cache_store
        unless cache_store_config.is_a?(Hash)
          cache_store_config = {::Rails.env => cache_store_config}
        end

        Switchman.config[:cache_map] = Engine.lookup_stores(cache_store_config)
      end

      # if the configured cache map (either from before, or as populated from
      # config.cache_store) didn't have an entry for Rails.env, add one using
      # lookup_store(nil); matches the behavior of Rails' default
      # initialize_cache initializer when config.cache_store is nil.
      unless Switchman.config[:cache_map].has_key?(::Rails.env)
        value = ::ActiveSupport::Cache.lookup_store(nil)
        Switchman.config[:cache_map][::Rails.env] = value
      end

      middlewares = Switchman.config[:cache_map].values.map do |store|
        value.middleware if value.respond_to?(:middleware)
      end.compact.uniq
      middlewares.each do |middleware|
        config.middleware.insert_before("Rack::Runtime", middleware)
      end

      # prevent :initialize_cache from trying to (or needing to) set
      # Rails.cache. once our switchman.extend_ar initializer (below) runs
      # Rails.cache will be overridden to pull appropriate values from the
      # cache map, but between now and then, Rails.cache should return the
      # Rails.env entry in the cache map.
      if ::Rails.version < '4'
        silence_warnings { Object.const_set "RAILS_CACHE", Switchman.config[:cache_map][::Rails.env] }
      else
        ::Rails.cache = Switchman.config[:cache_map][::Rails.env]
      end

      require "switchman/rails"
      ::Rails.send(:include, Rails)
    end

    initializer 'switchman.extend_ar', :before => "active_record.initialize_database" do
      ::ActiveSupport.on_load(:active_record) do
        require "switchman/active_record/abstract_adapter"
        require "switchman/active_record/association"
        require "switchman/active_record/attribute_methods"
        require "switchman/active_record/base"
        require "switchman/active_record/calculations"
        require "switchman/active_record/connection_handler"
        require "switchman/active_record/connection_pool"
        require "switchman/active_record/finder_methods"
        require "switchman/active_record/log_subscriber"
        require "switchman/active_record/persistence"
        require "switchman/active_record/query_cache"
        require "switchman/active_record/query_methods"
        require "switchman/active_record/relation"
        require "switchman/active_record/spawn_methods"

        include ActiveRecord::Base
        include ActiveRecord::AttributeMethods
        include ActiveRecord::Persistence
        ::ActiveRecord::Associations::Association.send(:include, ActiveRecord::Association)
        ::ActiveRecord::Associations::BelongsToAssociation.send(:include, ActiveRecord::BelongsToAssociation)
        ::ActiveRecord::Associations::CollectionProxy.send(:include, ActiveRecord::CollectionProxy)
        ::ActiveRecord::Associations::Builder::CollectionAssociation.send(:include, ActiveRecord::Builder::CollectionAssociation)

        ::ActiveRecord::Associations::Preloader::Association.send(:include, ActiveRecord::Preloader::Association)
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.send(:include, ActiveRecord::AbstractAdapter)
        ::ActiveRecord::ConnectionAdapters::ConnectionHandler.send(:include, ActiveRecord::ConnectionHandler)
        ::ActiveRecord::ConnectionAdapters::ConnectionPool.send(:include, ActiveRecord::ConnectionPool)
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.send(:include, ActiveRecord::QueryCache)
        # when we call super in Switchman::ActiveRecord::QueryCache#select_all,
        # we want it to find the definition from
        # ActiveRecord::ConnectionAdapters::DatabaseStatements, not
        # ActiveRecord::ConnectionAdapters::QueryCache
        ::ActiveRecord::ConnectionAdapters::QueryCache.send(:remove_method, :select_all)

        ::ActiveRecord::LogSubscriber.send(:include, ActiveRecord::LogSubscriber)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::Calculations)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::FinderMethods)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::QueryMethods)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::Relation)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::SpawnMethods)
      end
    end

    def self.foreign_key_check(name, type, options)
      if name.to_s =~ /_id\z/ && type.to_s == 'integer' && options[:limit].to_i < 8
        puts "WARNING: All foreign keys need to be 8-byte integers. #{name} looks like a foreign key. If so, please add the option: `:limit => 8`"
      end
    end

    initializer 'switchman.extend_connection_adapters', :after => "active_record.initialize_database" do
      ::ActiveSupport.on_load(:active_record) do
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.descendants.each do |klass|
          next if klass.instance_methods.include?(:add_column_with_foreign_key_check)
          klass.class_eval do
            def add_column_with_foreign_key_check(table, name, type, options = {})
              Switchman::Engine.foreign_key_check(name, type, options)
              add_column_without_foreign_key_check(table, name, type, options)
            end
            alias_method_chain(:add_column, :foreign_key_check)
          end
        end

        ::ActiveRecord::ConnectionAdapters::TableDefinition.class_eval do
          def column_with_foreign_key_check(name, type, options = {})
            Switchman::Engine.foreign_key_check(name, type, options)
            column_without_foreign_key_check(name, type, options)
          end
          alias_method_chain(:column, :foreign_key_check)
        end

        if defined?(::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
          require "switchman/active_record/postgresql_adapter"
          ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.send(:include, ActiveRecord::PostgreSQLAdapter)
        end
      end
    end

    initializer 'switchman.eager_load' do
      ::ActiveSupport.on_load(:before_eager_load) do
        # This needs to be loaded before Switchman::Shard, otherwise it won't autoload it correctly
        require 'active_record/base'
      end
    end

    initializer 'switchman.extend_shackles', :before => "switchman.extend_ar" do
      ::ActiveSupport.on_load(:active_record) do
        require "switchman/shackles"

        ::Shackles.send(:include, Shackles)
      end
    end

    initializer 'switchman.extend_controller', :after => "shackles.extend_ar" do
      ::ActiveSupport.on_load(:action_controller) do
        require "switchman/action_controller/caching"

        ::ActionController::Base.send(:include, ActionController::Caching)
      end
    end

    initializer 'switchman.set_reloader_hooks', :before => "active_record.set_reloader_hooks" do |app|
      ::ActiveSupport.on_load(:active_record) do
        ActionDispatch::Reloader.to_prepare do
          require_dependency 'switchman/default_shard'
        end
      end
    end
  end
end
