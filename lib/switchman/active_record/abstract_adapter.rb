module Switchman
  module ActiveRecord
    module AbstractAdapter
      attr_writer :shard

      def shard
        @shard || Shard.default
      end
    end
  end
end
