# frozen_string_literal: true

require 'switchman/errors'

module Switchman
  module ActiveRecord
    module ConnectionPool
      def shard
        shard_stack.last || Shard.default
      end

      def shard_stack
        unless (shard_stack = Thread.current.thread_variable_get(tls_key))
          shard_stack = Concurrent::Array.new
          Thread.current.thread_variable_set(tls_key, shard_stack)
        end
        shard_stack
      end

      def default_schema
        connection unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= connection.current_schemas
        @schemas.first
      end

      def checkout_new_connection
        conn = super
        conn.shard = shard
        conn
      end

      def connection(switch_shard: true)
        conn = super()
        raise NonExistentShardError if shard.new_record?

        switch_database(conn) if conn.shard != shard && switch_shard
        conn
      end

      def release_connection(with_id = Thread.current)
        super(with_id)

        flush
      end

      def remove_shard!(shard)
        synchronize do
          # The shard might be currently active, so we need to update our own shard
          self.shard = Shard.default if self.shard == shard
          # Update out any connections that may be using this shard
          @connections.each do |conn|
            # This will also update the connection's shard to the default shard
            switch_database(conn) if conn.shard == shard
          end
        end
      end

      def switch_database(conn)
        @schemas = conn.current_schemas if !@schemas && conn.adapter_name == 'PostgreSQL' && !shard.database_server.config[:shard_name]

        conn.shard = shard
      end

      private

      def tls_key
        "#{object_id}_shard".to_sym
      end
    end
  end
end
