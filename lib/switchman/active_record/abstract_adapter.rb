require 'switchman/sharded_instrumenter'

module Switchman
  module ActiveRecord
    module AbstractAdapter
      attr_writer :shard
      attr_reader :last_query_at

      def shard
        @shard || Shard.default
      end

      def initialize_with_shard(*args)
        initialize_without_shard(*args)
        @instrumenter = Switchman::ShardedInstrumenter.new(@instrumenter, self)
        @last_query_at = Time.now
      end

      def log_with_timestamp(*args, &block)
        log_without_timestamp(*args, &block)
      ensure
        @last_query_at = Time.now
      end

      def self.included(klass)
        klass.alias_method_chain :initialize, :shard unless klass.private_instance_methods.include?(:initialize_without_shard)
        klass.alias_method_chain :log, :timestamp unless klass.instance_methods.include?(:log_without_timestamp)
      end
    end
  end
end
