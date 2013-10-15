module Switchman
  class Engine < ::Rails::Engine
    isolate_namespace Switchman

    initializer 'switchman.extend_ar', :before => "active_record.initialize_database" do
      ActiveSupport.on_load(:active_record) do
        require "switchman/active_record/abstract_adapter"
        require "switchman/active_record/attribute_methods"
        require "switchman/active_record/base"
        require "switchman/active_record/calculations"
        require "switchman/active_record/connection_handler"
        require "switchman/active_record/connection_pool"
        require "switchman/active_record/finder_methods"
        require "switchman/active_record/log_subscriber"
        require "switchman/active_record/query_cache"
        require "switchman/active_record/query_methods"
        require "switchman/active_record/relation"
        require "switchman/cache_extensions"

        include ActiveRecord::Base
        include ActiveRecord::AttributeMethods
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.send(:include, ActiveRecord::AbstractAdapter)
        ::ActiveRecord::ConnectionAdapters::ConnectionHandler.send(:include, ActiveRecord::ConnectionHandler)
        ::ActiveRecord::ConnectionAdapters::ConnectionPool.send(:include, ActiveRecord::ConnectionPool)
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.send(:include, ActiveRecord::QueryCache)
        ::ActiveRecord::LogSubscriber.send(:include, ActiveRecord::LogSubscriber)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::Calculations)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::FinderMethods)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::QueryMethods)
        ::ActiveRecord::Relation.send(:include, ActiveRecord::Relation)
        Rails.send(:include, CacheExtensions)
      end
    end

    initializer 'switchman.extend_connection_adapters', :after => "active_record.initialize_database" do
      ActiveSupport.on_load(:active_record) do
        if defined?(::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)
          require "switchman/active_record/postgresql_adapter"
          ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.send(:include, ActiveRecord::PostgreSQLAdapter)
        end
      end
    end

    initializer 'switchman.eager_load' do
      ActiveSupport.on_load(:before_eager_load) do
        # This needs to be loaded before Switchman::Shard, otherwise it won't autoload it correctly
        require_dependency('active_record/base')
      end
    end

    initializer 'switchman.extend_shackles', :after => "shackles.extend_ar" do
      ActiveSupport.on_load(:active_record) do
        require "switchman/shackles"

        ::Shackles.send(:include, Shackles)
      end
    end
  end
end
