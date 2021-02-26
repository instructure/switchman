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
        ::ActiveRecord::Base.connection_pool.switch_database(conn) if conn.shard != ::ActiveRecord::Base.connection_pool.shard
        conn
      end
    end

    module Migrator
      def generate_migrator_advisory_lock_id
        shard_name_hash = Zlib.crc32("#{Shard.current.id}:#{Shard.current.name}")
        ::ActiveRecord::Migrator::MIGRATOR_SALT * shard_name_hash
      end
    end

    module MigrationContext
      def migrations
        return @migrations if instance_variable_defined?(:@migrations)

        migrations_cache = Thread.current[:migrations_cache] ||= {}
        key = Digest::MD5.hexdigest(migration_files.sort.join(','))
        @migrations = migrations_cache[key] ||= super
      end
    end
  end
end
