require 'switchman/sharded_instrumenter'

module Switchman
  module ActiveRecord
    module AbstractAdapter
      module ForeignKeyCheck
        def add_column(table, name, type, options = {})
          Engine.foreign_key_check(name, type, options)
          super
        end
      end

      attr_writer :shard
      attr_reader :last_query_at

      def shard
        @shard || Shard.default
      end

      def initialize(*args)
        super
        @instrumenter = Switchman::ShardedInstrumenter.new(@instrumenter, self)
        @last_query_at = Time.now
      end

      def quote_local_table_name(name)
        quote_table_name(name)
      end

      def use_qualified_names?
        false
      end

      protected

      def log(*args, &block)
        super
      ensure
        @last_query_at = Time.now
      end
    end
  end
end
