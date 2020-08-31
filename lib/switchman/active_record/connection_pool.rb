require 'switchman/errors'

module Switchman
  module ActiveRecord
    module ConnectionPool
      def shard
        Thread.current[tls_key] || Shard.default
      end

      def shard=(value)
        Thread.current[tls_key] = value
      end

      def default_schema
        raise "Not postgres!" unless self.spec.config[:adapter] == 'postgresql'
        connection unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= connection.current_schemas
        @schemas.first
      end

      def checkout_new_connection
        conn = synchronize do
          # ideally I would just keep a thread-local spec that I could modify
          # without locking anything, but if spec returns not-the-object passed
          # to initialize this pool, things break
          spec.config[:shard_name] = self.shard.name

          super
        end
        conn.shard = self.shard
        conn
      end

      def connection(switch_shard: true)
        conn = super()
        raise NonExistentShardError if shard.new_record?
        switch_database(conn) if conn.shard != self.shard && switch_shard
        conn
      end

      def release_connection(with_id = Thread.current)
        super(with_id)

        if spec.config[:idle_timeout]
          clear_idle_connections!(Time.now - spec.config[:idle_timeout].to_i)
        end
      end

      def remove_shard!(shard)
        synchronize do
          # The shard might be currently active, so we need to update our own shard
          if self.shard == shard
            self.shard = Shard.default
          end
          # Update out any connections that may be using this shard
          @connections.each do |conn|
            # This will also update the connection's shard to the default shard
            switch_database(conn) if conn.shard == shard
          end
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
        conn.shard = shard
      end

      private

      def tls_key
        "#{object_id}_shard".to_sym
      end
    end
  end
end
