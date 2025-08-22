# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module ConnectionPool
      if ::Rails.version < "7.1"
        def get_schema_cache(connection)
          self.schema_cache ||= SharedSchemaCache.get_schema_cache(connection)
          self.schema_cache.connection = connection

          self.schema_cache
        end

        # rubocop:disable Naming/AccessorMethodName -- override method
        def set_schema_cache(cache)
          schema_cache = get_schema_cache(cache.connection)

          cache.instance_variables.each do |x|
            schema_cache.instance_variable_set(x, cache.instance_variable_get(x))
          end
        end
        # rubocop:enable Naming/AccessorMethodName -- override method
      end

      def default_schema
        connection_method = (::Rails.version < "7.2") ? :connection : :lease_connection
        send(connection_method) unless @schemas
        # default shard will not switch databases immediately, so it won't be set yet
        @schemas ||= send(connection_method).current_schemas
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

      unless ::Rails.version < "7.2"
        def active_connection(switch_shard: true)
          conn = super()
          return nil if conn.nil?
          raise Errors::NonExistentShardError if current_shard.new_record?

          switch_database(conn) if conn.shard != current_shard && switch_shard
          conn
        end

        def lease_connection(switch_shard: true)
          conn = super()
          raise Errors::NonExistentShardError if current_shard.new_record?

          switch_database(conn) if conn.shard != current_shard && switch_shard
          conn
        end

        def with_connection(switch_shard: true, **kwargs)
          super(**kwargs) do |conn|
            raise Errors::NonExistentShardError if current_shard.new_record?

            switch_database(conn) if conn.shard != current_shard && switch_shard
            yield conn
          end
        end
      end

      def release_connection(with_id = Thread.current)
        super

        flush
      end

      def switch_database(conn)
        if !@schemas && conn.adapter_name == "PostgreSQL" && !current_shard.database_server.config[:shard_name]
          @schemas = conn.current_schemas
        end

        conn.shard = current_shard
      end

      private

      def current_shard
        if ::Rails.version < "8.0"
          connection_class.current_switchman_shard
        else
          connection_descriptor.name.constantize.current_switchman_shard
        end
      end

      def tls_key
        :"#{object_id}_shard"
      end
    end
  end
end
