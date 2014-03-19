module Switchman
  module ActiveRecord
    module ConnectionPool
      def self.included(klass)
        klass.alias_method_chain(:checkout_new_connection, :sharding)
        klass.alias_method_chain(:connection, :sharding)
      end

      attr_writer :shard

      def shard
        @shard || Shard.default
      end

      def default_schema
        raise "Not postgres!" unless self.spec.config[:adapter] == 'postgresql'
        connection unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= connection.schemas
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

      def switch_database(conn)
        if !@schemas && conn.adapter_name == 'PostgreSQL' && !self.shard.database_server.config[:shard_name]
          @schemas = conn.schemas
        end

        spec.config[:shard_name] = self.shard.name
        case conn.adapter_name
          when 'MySQL', 'Mysql2'
            conn.execute("USE #{spec.config[:database]}")
          when 'PostgreSQL'
            conn.schema_search_path = spec.config[:schema_search_path]
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
