module Switchman
  class ConnectionPoolProxy
    delegate :spec, :connected?, :default_schema, :with_connection,
             :to => :current_pool

    attr_reader :category

    def default_pool
      @default_pool
    end

    def initialize(category, default_pool, shard_connection_pools)
      @category = category
      @default_pool = default_pool
      @connection_pools = shard_connection_pools
    end

    def active_shard
      Shard.current(@category)
    end

    def current_pool
      pool = self.default_pool if active_shard.default?
      pool = @connection_pools[pool_key] ||= create_pool unless pool
      pool.shard = active_shard
      pool
    end

    def connection
      pool = current_pool
      pool.connection
    end

    %w{release_connection disconnect! clear_reloadable_connections! verify_active_connections! clear_stale_cached_connections!}.each do |method|
      class_eval(<<-EOS)
          def #{method}
            @connection_pools.values.each(&:#{method})
          end
      EOS
    end

    protected

    def pool_key
      active_shard.database_server.shareable? ? active_shard.database_server.id : active_shard
    end

    def create_pool
      shard = active_shard
      config = shard.database_server.config.dup
      spec = ::ActiveRecord::Base::ConnectionSpecification.new(config, "#{config[:adapter]}_connection")
      # unfortunately the AR code that does this require logic can't really be
      # called in isolation
      require "active_record/connection_adapters/#{config[:adapter]}_adapter"

      ::ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec).tap do |pool|
        pool.shard = shard
      end
    end
  end
end

