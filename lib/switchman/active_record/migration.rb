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
      # significant change: hash shard id, not database name
      def generate_migrator_advisory_lock_id
        shard_name_hash = Zlib.crc32(Shard.current.name)
        ::ActiveRecord::Migrator::MIGRATOR_SALT * shard_name_hash
      end

      if ::Rails.version >= '6.0'
        # copy/paste from Rails 6.1
        def with_advisory_lock
          lock_id = generate_migrator_advisory_lock_id

          with_advisory_lock_connection do |connection|
            got_lock = connection.get_advisory_lock(lock_id)
            raise ::ActiveRecord::ConcurrentMigrationError unless got_lock
            load_migrated # reload schema_migrations to be sure it wasn't changed by another process before we got the lock
            yield
          ensure
            if got_lock && !connection.release_advisory_lock(lock_id)
              raise ::ActiveRecord::ConcurrentMigrationError.new(
                ::ActiveRecord::ConcurrentMigrationError::RELEASE_LOCK_FAILED_MESSAGE
              )
            end
          end
        end

        # significant change: strip out prefer_secondary from config
        def with_advisory_lock_connection
          pool = ::ActiveRecord::ConnectionAdapters::ConnectionHandler.new.establish_connection(
            ::ActiveRecord::Base.connection_config.except(:prefer_secondary)
          )

          pool.with_connection { |connection| yield(connection) }
        ensure
          pool&.disconnect!
        end
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
