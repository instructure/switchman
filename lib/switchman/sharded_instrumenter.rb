# frozen_string_literal: true

module Switchman
  class ShardedInstrumenter < ::SimpleDelegator
    def initialize(instrumenter, shard_host)
      super(instrumenter)
      @shard_host = shard_host
    end

    def instrument(name, payload = {})
      shard = @shard_host&.shard
      # attribute_methods_generated? will be false during a reload -
      # when we might be doing a query while defining attribute methods,
      # so just avoid logging then
      if shard.is_a?(Shard) && Shard.instance_variable_get(:@attribute_methods_generated)
        env = if ::Rails.version < "8.0"
                @shard_host.pool.connection_class&.current_role
              else
                @shard_host.pool.connection_descriptor.name.constantize&.current_role
              end

        payload[:shard] = {
          database_server_id: shard.database_server.id,
          id: shard.id,
          env: env
        }
      end
      super
    end
  end
end
