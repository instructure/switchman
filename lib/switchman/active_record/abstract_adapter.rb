# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module AbstractAdapter
      module ForeignKeyCheck
        def add_column(table, name, type, limit: nil, **)
          Switchman.foreign_key_check(name, type, limit:)
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

        @instrumenter = Switchman::ShardedInstrumenter.new(@instrumenter, self) if ::Rails.version < "8.0"

        @last_query_at = Time.now
      end

      if ::Rails.version >= "8.0"
        def instrumenter # :nodoc:
          @instrumenter ||= Switchman::ShardedInstrumenter.new(::ActiveSupport::Notifications.instrumenter, self)
        end
      end

      def quote_local_table_name(name)
        quote_table_name(name)
      end

      protected

      def log(...)
        super
      ensure
        @last_query_at = Time.now
      end
    end
  end
end
