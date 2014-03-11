module Switchman
  module ActiveRecord
    module ConnectionPool
      def self.included(klass)
        klass.alias_method_chain(:checkout_new_connection, :sharding)
        klass.send(:remove_method, :connection)
        klass.send(:remove_method, :active_connection?)
        klass.send(:remove_method, :release_connection)
        klass.send(:remove_method, :clear_stale_cached_connections!)
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

      def connection
        conns = synchronize { @reserved_connections[current_connection_id] ||= [] }
        conn = conns.find { |conn| !conn.open_transactions || conn.shard == self.shard || conn.adapter_name == 'PostgreSQL' }
        unless conn
          conn = checkout
          yield conn if block_given?
          conns << conn
        end
        switch_database(conn) if conn.shard != self.shard
        conn
      end

      # Is there an open connection that is being used for the current thread?
      def active_connection?
        synchronize do
          @reserved_connections.fetch(current_connection_id) {
            return false
          }.any? { |conn| conn.in_use? }
        end
      end

      def release_connection
        conns = synchronize { @reserved_connections.delete(current_connection_id) }
        conns.each { |conn| checkin conn } if conns
      end

      def clear_stale_cached_connections!
        keys = @reserved_connections.keys - Thread.list.find_all { |t|
          t.alive?
        }.map { |thread| thread.object_id }
        keys.each do |key|
          conns = @reserved_connections[key]
          conns.each do |conn|
            ::ActiveSupport::Deprecation.warn(<<-eowarn) if conn.in_use?
Database connections will not be closed automatically, please close your
database connection at the end of the thread by calling `close` on your
connection.  For example: ActiveRecord::Base.connection.close
            eowarn
            checkin conn
          end
          @reserved_connections.delete(key)
        end
      end

      def release(conn)
        synchronize do
          thread_id = nil

          if @reserved_connections[current_connection_id].include?(conn)
            thread_id = current_connection_id
            @reserved_connections[thread_id].delete(conn)
          else
            thread_id = @reserved_connections.keys.find { |k|
              @reserved_connections[k].include?(conn)
            }
            if thread_id
              @reserved_connections[thread_id].delete(conn)
            end
          end

          @reserved_connections.delete thread_id if thread_id && @reserved_connections[thread_id].empty?
        end
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
