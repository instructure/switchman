module Switchman
  class SchemaCache < ::ActiveRecord::ConnectionAdapters::SchemaCache
    delegate :connection, to: :pool
    attr_reader :pool

    def initialize(pool)
      @pool = pool
      super(nil)
    end
  end
end
