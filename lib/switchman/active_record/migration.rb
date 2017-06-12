module Switchman
  module ActiveRecord
    module Migration
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
  end
end
