# frozen_string_literal: true

require 'switchman/errors'

module Switchman
  module ActiveRecord
    module ConnectionPool
      def shard
        shard_stack.last || Shard.default
      end

      def shard_stack
        if shard_stack = Thread.current.thread_variable_get(tls_key)
          shard_stack
        else
          shard_stack = Concurrent::Array.new
          Thread.current.thread_variable_set(tls_key, shard_stack)
          shard_stack
        end
      end

      def default_schema
        connection unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= connection.current_schemas
        @schemas.first
      end

      def checkout_new_connection
        conn = super
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

        flush
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

      def switch_database(conn)
        if !@schemas && conn.adapter_name == 'PostgreSQL' && !self.shard.database_server.config[:shard_name]
          @schemas = conn.current_schemas
        end

        conn.shard = shard
      end

      private

      def tls_key
        "#{object_id}_shard".to_sym
      end
    end
  end
end
