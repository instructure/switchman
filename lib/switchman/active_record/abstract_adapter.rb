# frozen_string_literal: true

require 'switchman/sharded_instrumenter'

module Switchman
  module ActiveRecord
    module AbstractAdapter
      module ForeignKeyCheck
        def add_column(table, name, type, limit: nil, **)
          Engine.foreign_key_check(name, type, limit: limit)
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

      def schema_migration
        ::ActiveRecord::SchemaMigration
      end

      protected

      def log(*args, &block)
        super
      ensure
        @last_query_at = Time.now
      end

      private

      def id_value_for_database(value)
        return super unless value.class.sharded_primary_key?

        # do this the Rails 4.2 way, so that if Shard.current != self.shard, the id gets transposed
        quote(value.id)
      end
    end
  end
end
