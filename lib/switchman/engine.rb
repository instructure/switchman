# frozen_string_literal: true

module Switchman
  class Engine < ::Rails::Engine
    isolate_namespace Switchman

    # enable Rails 6.1 style connection handling
    config.active_record.legacy_connection_handling = false
    config.active_record.writing_role = :primary

    config.autoload_once_paths << File.expand_path('app/models', config.paths.path)

    def self.lookup_stores(cache_store_config)
      result = {}
      cache_store_config.each do |key, value|
        next if value.is_a?(String)

        result[key] = ::ActiveSupport::Cache.lookup_store(value)
      end

      cache_store_config.each do |key, value| # rubocop:disable Style/CombinableLoops
        next unless value.is_a?(String)

        result[key] = result[value]
      end
      result
    end

    initializer 'switchman.initialize_cache', before: 'initialize_cache' do
      require 'switchman/active_support/cache'
      ::ActiveSupport::Cache.singleton_class.prepend(ActiveSupport::Cache::ClassMethods)

      # if we haven't already setup our cache map out-of-band, set it up from
      # config.cache_store now. behaves similarly to Rails' default
      # initialize_cache initializer, but for each value in the map, rather
      # than just Rails.cache. if config.cache_store is a flat value, uses it
      # to fill just the Rails.env entry in the cache map.
      unless Switchman.config[:cache_map].present?
        cache_store_config = ::Rails.configuration.cache_store
        cache_store_config = { ::Rails.env => cache_store_config } unless cache_store_config.is_a?(Hash)

        Switchman.config[:cache_map] = Engine.lookup_stores(cache_store_config)
      end

      # if the configured cache map (either from before, or as populated from
      # config.cache_store) didn't have an entry for Rails.env, add one using
      # lookup_store(nil); matches the behavior of Rails' default
      # initialize_cache initializer when config.cache_store is nil.
      unless Switchman.config[:cache_map].key?(::Rails.env)
        value = ::ActiveSupport::Cache.lookup_store(nil)
        Switchman.config[:cache_map][::Rails.env] = value
      end

      middlewares = Switchman.config[:cache_map].values.map do |store|
        store.middleware if store.respond_to?(:middleware)
      end.compact.uniq
      middlewares.each do |middleware|
        config.middleware.insert_before('Rack::Runtime', middleware)
      end

      # prevent :initialize_cache from trying to (or needing to) set
      # Rails.cache. once our switchman.extend_ar initializer (below) runs
      # Rails.cache will be overridden to pull appropriate values from the
      # cache map, but between now and then, Rails.cache should return the
      # Rails.env entry in the cache map.
      ::Rails.cache = Switchman.config[:cache_map][::Rails.env]
    end

    initializer 'switchman.extend_ar', before: 'active_record.initialize_database' do
      ::ActiveSupport.on_load(:active_record) do
        require 'switchman/active_record/abstract_adapter'
        require 'switchman/active_record/association'
        require 'switchman/active_record/attribute_methods'
        require 'switchman/active_record/base'
        require 'switchman/active_record/calculations'
        require 'switchman/active_record/connection_pool'
        require 'switchman/active_record/database_configurations'
        require 'switchman/active_record/database_configurations/database_config'
        require 'switchman/active_record/finder_methods'
        require 'switchman/active_record/log_subscriber'
        require 'switchman/active_record/migration'
        require 'switchman/active_record/model_schema'
        require 'switchman/active_record/persistence'
        require 'switchman/active_record/predicate_builder'
        require 'switchman/active_record/query_cache'
        require 'switchman/active_record/query_methods'
        require 'switchman/active_record/reflection'
        require 'switchman/active_record/relation'
        require 'switchman/active_record/spawn_methods'
        require 'switchman/active_record/statement_cache'
        require 'switchman/active_record/tasks/database_tasks'
        require 'switchman/active_record/type_caster'
        require 'switchman/arel'
        require 'switchman/call_super'
        require 'switchman/rails'
        require 'switchman/guard_rail/relation'
        require 'switchman/standard_error'

        ::StandardError.include(StandardError)

        self.default_shard = ::Rails.env.to_sym
        self.default_role = :primary

        prepend ActiveRecord::Base
        include ActiveRecord::AttributeMethods
        include ActiveRecord::Persistence
        singleton_class.prepend ActiveRecord::ModelSchema::ClassMethods

        ::ActiveRecord::StatementCache.prepend(ActiveRecord::StatementCache)
        ::ActiveRecord::StatementCache.singleton_class.prepend(ActiveRecord::StatementCache::ClassMethods)
        ::ActiveRecord::StatementCache::BindMap.prepend(ActiveRecord::StatementCache::BindMap)
        ::ActiveRecord::StatementCache::Substitute.send(:attr_accessor, :primary, :sharded)

        ::ActiveRecord::Associations::CollectionAssociation.prepend(ActiveRecord::CollectionAssociation)
        ::ActiveRecord::Associations::HasOneAssociation.prepend(ActiveRecord::ForeignAssociation)
        ::ActiveRecord::Associations::HasManyAssociation.prepend(ActiveRecord::ForeignAssociation)

        ::ActiveRecord::PredicateBuilder.singleton_class.prepend(ActiveRecord::PredicateBuilder)

        prepend(ActiveRecord::AutosaveAssociation)

        ::ActiveRecord::Associations::Association.prepend(ActiveRecord::Association)
        ::ActiveRecord::Associations::BelongsToAssociation.prepend(ActiveRecord::BelongsToAssociation)
        ::ActiveRecord::Associations::CollectionProxy.include(ActiveRecord::CollectionProxy)

        ::ActiveRecord::Associations::Preloader::Association.prepend(ActiveRecord::Preloader::Association)
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.prepend(ActiveRecord::AbstractAdapter)
        ::ActiveRecord::ConnectionAdapters::ConnectionPool.prepend(ActiveRecord::ConnectionPool)
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.prepend(ActiveRecord::QueryCache)

        ::ActiveRecord::DatabaseConfigurations.prepend(ActiveRecord::DatabaseConfigurations)
        ::ActiveRecord::DatabaseConfigurations::DatabaseConfig.prepend(ActiveRecord::DatabaseConfigurations::DatabaseConfig)

        ::ActiveRecord::LogSubscriber.prepend(ActiveRecord::LogSubscriber)
        ::ActiveRecord::Migration.prepend(ActiveRecord::Migration)
        ::ActiveRecord::Migration::Compatibility::V5_0.prepend(ActiveRecord::Migration::Compatibility::V5_0)
        ::ActiveRecord::MigrationContext.prepend(ActiveRecord::MigrationContext)
        ::ActiveRecord::Migrator.prepend(ActiveRecord::Migrator)

        ::ActiveRecord::Reflection::AbstractReflection.include(ActiveRecord::Reflection::AbstractReflection)
        ::ActiveRecord::Reflection::AssociationReflection.prepend(ActiveRecord::Reflection::AssociationScopeCache)
        ::ActiveRecord::Reflection::ThroughReflection.prepend(ActiveRecord::Reflection::AssociationScopeCache)
        ::ActiveRecord::Reflection::AssociationReflection.prepend(ActiveRecord::Reflection::AssociationReflection)
        ::ActiveRecord::Relation.prepend(ActiveRecord::Calculations)
        ::ActiveRecord::Relation.include(ActiveRecord::FinderMethods)
        ::ActiveRecord::Relation.include(ActiveRecord::QueryMethods)
        ::ActiveRecord::Relation.prepend(GuardRail::Relation)
        ::ActiveRecord::Relation.prepend(ActiveRecord::Relation)
        ::ActiveRecord::Relation.include(ActiveRecord::SpawnMethods)
        ::ActiveRecord::Relation.include(CallSuper)

        ::ActiveRecord::PredicateBuilder::AssociationQueryValue.prepend(ActiveRecord::PredicateBuilder::AssociationQueryValue)
        ::ActiveRecord::PredicateBuilder::PolymorphicArrayValue.prepend(ActiveRecord::PredicateBuilder::AssociationQueryValue)

        ::ActiveRecord::Tasks::DatabaseTasks.singleton_class.prepend(ActiveRecord::Tasks::DatabaseTasks)

        ::ActiveRecord::TypeCaster::Map.include(ActiveRecord::TypeCaster::Map)
        ::ActiveRecord::TypeCaster::Connection.include(ActiveRecord::TypeCaster::Connection)

        ::Rails.singleton_class.prepend(Rails::ClassMethods)

        ::Arel::Table.prepend(Arel::Table)
        ::Arel::Visitors::ToSql.prepend(Arel::Visitors::ToSql)
      end
    end

    def self.foreign_key_check(name, type, limit: nil)
      puts "WARNING: All foreign keys need to be 8-byte integers. #{name} looks like a foreign key. If so, please add the option: `:limit => 8`" if name.to_s =~ /_id\z/ && type.to_s == 'integer' && limit.to_i < 8
    end

    initializer 'switchman.extend_connection_adapters', after: 'active_record.initialize_database' do
      ::ActiveSupport.on_load(:active_record) do
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.descendants.each do |klass|
          klass.prepend(ActiveRecord::AbstractAdapter::ForeignKeyCheck)
        end

        require 'switchman/active_record/table_definition'
        ::ActiveRecord::ConnectionAdapters::TableDefinition.prepend(ActiveRecord::TableDefinition)

        if defined?(::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
          require 'switchman/active_record/postgresql_adapter'
          ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(ActiveRecord::PostgreSQLAdapter)
        end

        Shard.send(:initialize_sharding)
      end
    end

    initializer 'switchman.eager_load' do
      ::ActiveSupport.on_load(:before_eager_load) do
        # This needs to be loaded before Switchman::Shard, otherwise it won't autoload it correctly
        require 'active_record/base'
      end
    end

    initializer 'switchman.extend_guard_rail', before: 'switchman.extend_ar' do
      ::ActiveSupport.on_load(:active_record) do
        require 'switchman/guard_rail'

        ::GuardRail.singleton_class.prepend(GuardRail::ClassMethods)
      end
    end

    initializer 'switchman.extend_controller', after: 'guard_rail.extend_ar' do
      ::ActiveSupport.on_load(:action_controller) do
        require 'switchman/action_controller/caching'

        ::ActionController::Base.include(ActionController::Caching)
      end
    end
  end
end
