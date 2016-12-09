module Switchman
  class ShardedInstrumenter < ::SimpleDelegator
    def initialize(instrumenter, shard_host)
      super instrumenter
      @shard_host = shard_host
    end

    def instrument(name, payload={})
      shard = @shard_host&.shard
      # attribute_methods_generated? will be false during a reload -
      # when we might be doing a query while defining attribute methods,
      # so just avoid logging then
      if shard.is_a?(Shard) && Shard.instance_variable_get(:@attribute_methods_generated)
        payload[:shard] = {
          database_server_id: shard.database_server.id,
          id: shard.id,
          env: shard.database_server.shackles_environment
        }
      end
      super name, payload
    end
  end
end
