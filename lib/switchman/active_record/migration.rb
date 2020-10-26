# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Migration
      module Compatibility
        module V5_0
          def create_table(*args, **options)
            unless options.key?(:id)
              options[:id] = :bigserial
            end
            if block_given?
              super do |td|
                yield td
              end
            else
              super
            end
          end
        end
      end

      def connection
        conn = super
        if conn.shard != ::ActiveRecord::Base.connection_pool.current_pool.shard
          ::ActiveRecord::Base.connection_pool.current_pool.switch_database(conn)
        end
        conn
      end
    end

    module Migrator
      def generate_migrator_advisory_lock_id
        shard_name_hash = Zlib.crc32(Shard.current.name)
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
