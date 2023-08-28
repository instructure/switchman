# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module ConnectionHandler
      def resolve_pool_config(config, connection_name, role, shard)
        ret = super
        # Make *all* pool configs use the same schema reflection
        ret.schema_reflection = ConnectionHandler.global_schema_reflection
        ret
      end

      def self.global_schema_reflection
        @global_schema_reflection ||= ::ActiveRecord::ConnectionAdapters::SchemaReflection.new(nil)
      end
    end
  end
end
