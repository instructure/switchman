# frozen_string_literal: true

module Switchman
  class SharedSchemaCache
    def self.get_schema_cache(connection)
      @schema_cache ||= ::ActiveRecord::ConnectionAdapters::SchemaCache.new(connection)
      @schema_cache.connection = connection
      @schema_cache
    end
  end
end
