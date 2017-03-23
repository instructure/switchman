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
  end
end
