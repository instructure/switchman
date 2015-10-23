require 'switchman/sharded_instrumenter'

module Switchman
  module ActiveRecord
    module AbstractAdapter
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

      def log(*args, &block)
        super
      ensure
        @last_query_at = Time.now
      end

      def quote_local_table_name(name)
        quote_table_name(name)
      end

      if ::Rails.version < '4'
        def dump_schema_information #:nodoc:
          sm_table = ::ActiveRecord::Migrator.schema_migrations_table_name
          migrated = select_values("SELECT version FROM #{quote_table_name(sm_table)} ORDER BY version")
          migrated.map { |v| "INSERT INTO #{quote_table_name(sm_table)} (version) VALUES ('#{v}');" }.join("\n\n")
        end
      end
    end
  end
end
