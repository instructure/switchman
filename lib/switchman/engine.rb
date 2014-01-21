module Switchman
  class Engine < ::Rails::Engine
    isolate_namespace Switchman

    initializer 'switchman.extend_ar', :before => "active_record.initialize_database" do
      ActiveSupport.on_load(:active_record) do
        #require 'active_record/associations/preloader/belongs_to'

        require "switchman/active_record/abstract_adapter"
        require "switchman/active_record/association"
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
        ::ActiveRecord::Associations::Association.send(:include, ActiveRecord::Association)
        ::ActiveRecord::Associations::BelongsToAssociation.send(:include, ActiveRecord::BelongsToAssociation)
        ::ActiveRecord::Associations::CollectionProxy.send(:include, ActiveRecord::CollectionProxy)
        ::ActiveRecord::Associations::Builder::Association.send(:include, ActiveRecord::Builder::Association)

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
        Rails.send(:include, CacheExtensions)
      end
    end

    def self.foreign_key_check(name, type, options)
      if name.to_s =~ /_id\z/ && type.to_s == 'integer' && options[:limit].to_i < 8
        puts "WARNING: All foreign keys need to be 8-byte integers. #{name} looks like a foreign key. If so, please add the option: `:limit => 8`"
      end
    end

    initializer 'switchman.extend_connection_adapters', :after => "active_record.initialize_database" do
      ActiveSupport.on_load(:active_record) do
        ::ActiveRecord::ConnectionAdapters::AbstractAdapter.descendants.each do |klass|
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
