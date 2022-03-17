# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module ConnectionPool
      def default_schema
        connection unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= connection.current_schemas
        @schemas.first
      end

      def checkout_new_connection
        conn = super
        conn.shard = current_shard
        conn
      end

      def connection(switch_shard: true)
        conn = super()
        raise Errors::NonExistentShardError if current_shard.new_record?

        switch_database(conn) if conn.shard != current_shard && switch_shard
        conn
      end

      def release_connection(with_id = Thread.current)
        super(with_id)

        flush
      end

      def switch_database(conn)
        @schemas = conn.current_schemas if !@schemas && conn.adapter_name == 'PostgreSQL' && !current_shard.database_server.config[:shard_name]

        conn.shard = current_shard
      end

      private

      def current_shard
        ::Rails.version < '7.0' ? connection_klass.current_switchman_shard : connection_class.current_switchman_shard
      end

      def tls_key
        "#{object_id}_shard".to_sym
      end
    end
  end
end
