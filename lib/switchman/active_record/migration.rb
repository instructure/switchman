# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Migration
      module Compatibility
        module V5_0 # rubocop:disable Naming/ClassAndModuleCamelCase
          def create_table(*args, **options)
            options[:id] = :bigserial unless options.key?(:id)
            super
          end
        end
      end

      def connection
        conn = super
        if conn.shard != ::ActiveRecord::Base.current_switchman_shard
          ::ActiveRecord::Base.connection_pool.switch_database(conn)
        end
        conn
      end
    end

    module Migrator
      # significant change: use the shard name instead of the database name
      # in the lock id. Especially if you're going through pgbouncer, the
      # database name you're accessing may not be consistent
      def generate_migrator_advisory_lock_id
        db_name_hash = Zlib.crc32(Shard.current.name)
        shard_name_hash = ::ActiveRecord::Migrator::MIGRATOR_SALT * db_name_hash
        # Store in internalmetadata to allow other tools to be able to lock out migrations
        ::ActiveRecord::InternalMetadata[:migrator_advisory_lock_id] = shard_name_hash
        shard_name_hash
      end

      # significant change: strip out prefer_secondary from config
      def with_advisory_lock_connection
        pool = ::ActiveRecord::ConnectionAdapters::ConnectionHandler.new.establish_connection(
          ::ActiveRecord::Base.connection_db_config.configuration_hash.except(:prefer_secondary)
        )

        pool.with_connection { |connection| yield(connection) } # rubocop:disable Style/ExplicitBlockArgument
      ensure
        pool&.disconnect!
      end
    end

    module MigrationContext
      def migrate(...)
        connection = ::ActiveRecord::Base.connection
        connection_pool = ::ActiveRecord::Base.connection_pool
        previous_schema_cache = connection_pool.get_schema_cache(connection)
        temporary_schema_cache = ::ActiveRecord::ConnectionAdapters::SchemaCache.new(connection)

        reset_column_information
        connection_pool.set_schema_cache(temporary_schema_cache)

        begin
          super(...)
        ensure
          connection_pool.set_schema_cache(previous_schema_cache)
          reset_column_information
        end
      end

      def migrations
        return @migrations if instance_variable_defined?(:@migrations)

        migrations_cache = Thread.current[:migrations_cache] ||= {}
        key = Digest::MD5.hexdigest(migration_files.sort.join(","))
        @migrations = migrations_cache[key] ||= super
      end

      private

      def reset_column_information
        ::ActiveRecord::Base.descendants.reject { |m| m <= UnshardedRecord }.each(&:reset_column_information)
      end
    end
  end
end
