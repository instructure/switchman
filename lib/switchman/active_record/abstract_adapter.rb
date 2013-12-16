require 'switchman/sharded_instrumenter'

module Switchman
  module ActiveRecord
    module AbstractAdapter
      attr_writer :shard

      def shard
        @shard || Shard.default
      end

      def initialize_with_shard(*args)
        initialize_without_shard(*args)
        @instrumenter = Switchman::ShardedInstrumenter.new(@instrumenter, self)
      end

      def self.included(klass)
        klass.alias_method_chain :initialize, :shard
      end
    end
  end
end
