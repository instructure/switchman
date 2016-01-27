module Switchman
  module ActiveRecord
    module ConnectionPool
      def self.included(klass)
        klass.alias_method_chain(:checkout_new_connection, :sharding)
        klass.alias_method_chain(:connection, :sharding)
        klass.alias_method_chain(:release_connection, :idle_timeout)
      end

      attr_writer :shard

      def shard
        @shard || Shard.default
      end

      def default_schema
        raise "Not postgres!" unless self.spec.config[:adapter] == 'postgresql'
        connection unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= connection.current_schemas
        @schemas.first
      end

      def checkout_new_connection_with_sharding
        # TODO: this might be a threading issue
        spec.config[:shard_name] = self.shard.name

        conn = checkout_new_connection_without_sharding
        conn.shard = self.shard
        conn
      end

      def connection_with_sharding
        conn = connection_without_sharding
        switch_database(conn) if conn.shard != self.shard
        conn
      end

      def release_connection_with_idle_timeout(with_id = current_connection_id)
        release_connection_without_idle_timeout(with_id)

        if spec.config[:idle_timeout]
          clear_idle_connections!(Time.now - spec.config[:idle_timeout].to_i)
        end
      end

      def clear_idle_connections!(since_when)
        synchronize do
          @connections.reject! do |conn|
            if conn.last_query_at < since_when && !conn.in_use?
              conn.disconnect!
              true
            else
              false
            end
          end
          @available.clear
          @connections.each do |conn|
            @available.add conn
          end
        end
      end

      def switch_database(conn)
        if !@schemas && conn.adapter_name == 'PostgreSQL' && !self.shard.database_server.config[:shard_name]
          @schemas = conn.current_schemas
        end

        spec.config[:shard_name] = self.shard.name
        case conn.adapter_name
          when 'MySQL', 'Mysql2'
            conn.execute("USE #{spec.config[:database]}")
          when 'PostgreSQL'
            if conn.schema_search_path != spec.config[:schema_search_path]
              conn.schema_search_path = spec.config[:schema_search_path]
            end
          when 'SQLite'
            # This is an artifact of the adapter modifying the path to be an absolute path when it is instantiated; just let it slide
          else
            raise("Cannot switch databases on same DatabaseServer with adapter type: #{conn.adapter_name}. Limit one Shard per DatabaseServer.")
        end
        conn.shard = shard
      end
    end
  end
end
