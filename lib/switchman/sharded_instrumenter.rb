module Switchman
  class ShardedInstrumenter < ::SimpleDelegator
    def initialize(instrumenter, shard_host)
      super instrumenter
      @shard_host = shard_host
    end

    def instrument(name, payload={})
      shard = @shard_host.try(:shard)
      if shard.is_a?(Shard)
        payload[:shard] = {
          id: shard.id,
          env: shard.database_server.shackles_environment
        }
      end
      super name, payload
    end
  end
end
