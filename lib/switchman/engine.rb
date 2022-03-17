# frozen_string_literal: true

module Switchman
  class Engine < ::Rails::Engine
    isolate_namespace Switchman

    # enable Rails 6.1 style connection handling
    config.active_record.legacy_connection_handling = false
    config.active_record.writing_role = :primary

    ::GuardRail.singleton_class.prepend(GuardRail::ClassMethods)

    # after :initialize_dependency_mechanism to ensure autoloading is configured for any downstream initializers that care
    # In rails 7.0 we should be able to just use an explicit after on configuring the once autoloaders and not need to go monkey around with initializer order
    if ::Rails.version < '7.0'
      initialize_dependency_mechanism = ::Rails::Application::Bootstrap.initializers.find { |i| i.name == :initialize_dependency_mechanism }
      initialize_dependency_mechanism.instance_variable_get(:@options)[:after] = :set_autoload_paths
    end

    initializer 'switchman.active_record_patch',
                before: 'active_record.initialize_database',
                after: (::Rails.version < '7.0' ? :initialize_dependency_mechanism : :setup_once_autoloader) do
      ::ActiveSupport.on_load(:active_record) do
        # Switchman requires postgres, so just always load the pg adapter
        require 'active_record/connection_adapters/postgresql_adapter'

        self.default_shard = ::Rails.env.to_sym
        self.default_role = :primary

        prepend ActiveRecord::Base
        prepend ActiveRecord::AttributeMethods
        include ActiveRecord::Persistence
        singleton_class.prepend ActiveRecord::ModelSchema::ClassMethods

        ::ActiveRecord::StatementCache.prepend(ActiveRecord::StatementCache)
        ::ActiveRecord::StatementCache.singleton_class.prepend(ActiveRecord::StatementCache::ClassMethods)
        ::ActiveRecord::StatementCache::BindMap.prepend(ActiveRecord::StatementCache::BindMap)
        ::ActiveRecord::StatementCache::Substitute.send(:attr_accessor, :primary, :sharded)

        ::ActiveRecord::Associations::CollectionAssociation.prepend(ActiveRecord::Associations::CollectionAssociation)
        ::ActiveRecord::Associations::HasOneAssociation.prepend(ActiveRecord::Associations::ForeignAssociation)
        ::ActiveRecord::Associations::HasManyAssociation.prepend(ActiveRecord::Associations::ForeignAssociation)

        ::ActiveRecord::PredicateBuilder.singleton_class.prepend(ActiveRecord::PredicateBuilder)

        prepend(ActiveRecord::Associations::AutosaveAssociation)

        ::ActiveRecord::Associations::Association.prepend(ActiveRecord::Associations::Association)
        ::ActiveRecord::Associations::BelongsToAssociation.prepend(ActiveRecord::Associations::BelongsToAssociation)
        ::ActiveRecord::Associations::CollectionProxy.include(ActiveRecord::Associations::CollectionProxy)

        ::ActiveRecord::Associations::Preloader::Association.prepend(ActiveRecord::Associations::Preloader::Association)
        ::ActiveRecord::Associations::Preloader::Association::LoaderQuery.prepend(ActiveRecord::Associations::Preloader::Association::LoaderQuery) unless ::Rails.version < '7.0'
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.prepend(ActiveRecord::AbstractAdapter)
        ::ActiveRecord::ConnectionAdapters::ConnectionPool.prepend(ActiveRecord::ConnectionPool)
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.prepend(ActiveRecord::QueryCache)
        ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(ActiveRecord::PostgreSQLAdapter)

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

        ::ActiveRecord::TestFixtures.prepend(ActiveRecord::TestFixtures)

        ::ActiveRecord::TypeCaster::Map.include(ActiveRecord::TypeCaster::Map)
        ::ActiveRecord::TypeCaster::Connection.include(ActiveRecord::TypeCaster::Connection)

        ::Arel::Table.prepend(Arel::Table)
        ::Arel::Visitors::ToSql.prepend(Arel::Visitors::ToSql)

        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.descendants.each do |klass|
          klass.prepend(ActiveRecord::AbstractAdapter::ForeignKeyCheck)
        end

        ::ActiveRecord::ConnectionAdapters::TableDefinition.prepend(ActiveRecord::TableDefinition)
      end
      # Ensure that ActiveRecord::Base is always loaded before any app-level initializers can go try to load Switchman::Shard or we get a loop
      ::ActiveRecord::Base
    end

    initializer 'switchman.error_patch', after: 'active_record.initialize_database' do
      ::ActiveSupport.on_load(:active_record) do
        ::StandardError.include(StandardError)
      end
    end

    initializer 'switchman.initialize_cache', before: :initialize_cache, after: 'active_record.initialize_database' do
      ::ActiveSupport::Cache.singleton_class.prepend(ActiveSupport::Cache::ClassMethods)

      # if we haven't already setup our cache map out-of-band, set it up from
      # config.cache_store now. behaves similarly to Rails' default
      # initialize_cache initializer, but for each value in the map, rather
      # than just Rails.cache. if config.cache_store is a flat value, uses it
      # to fill just the Rails.env entry in the cache map.
      unless Switchman.config[:cache_map].present?
        cache_store_config = ::Rails.configuration.cache_store
        cache_store_config = { ::Rails.env => cache_store_config } unless cache_store_config.is_a?(Hash)

        Switchman.config[:cache_map] = ::ActiveSupport::Cache.lookup_stores(cache_store_config)
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
      ::Rails.singleton_class.prepend(Rails::ClassMethods)

      ::ActiveSupport.on_load(:action_controller) do
        ::ActionController::Base.include(ActionController::Caching)
      end
    end
  end
end
