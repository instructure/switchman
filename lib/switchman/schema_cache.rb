module Switchman
  class SchemaCache < ::ActiveRecord::ConnectionAdapters::SchemaCache
    delegate :connection, to: :pool
    attr_reader :pool

    def initialize(pool)
      @pool = pool
      super(nil)
    end

    def copy_values(other_cache)
      # use the same cached values but still fall back to the correct pool
      [:@columns, :@columns_hash, :@primary_keys, :@data_sources].each do |iv|
        instance_variable_set(iv, other_cache.instance_variable_get(iv))
      end
    end
  end
end
