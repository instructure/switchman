require 'switchman/schema_cache'

module Switchman
  module ConnectionError
    def self.===(other)
      return true if defined?(PG::Error) && PG::Error === other
      return true if defined?(Mysql2::Error) && Mysql2::Error === other
      false
    end
  end

  class ConnectionPoolProxy
    delegate :spec, :connected?, :default_schema, :with_connection,
             :to => :current_pool

    attr_reader :category, :schema_cache

    def default_pool
      @default_pool
    end

    def initialize(category, default_pool, shard_connection_pools)
      @category = category
      @default_pool = default_pool
      @connection_pools = shard_connection_pools
      @schema_cache = SchemaCache.new(self)
    end

    def active_shard
      Shard.current(@category)
    end

    def active_shackles_environment
      ::Rails.env.test? ? :master : active_shard.database_server.shackles_environment
    end

    def current_pool
      pool = self.default_pool if active_shard.database_server == Shard.default.database_server && active_shackles_environment == :master && (active_shard == Shard.default || active_shard.database_server.shareable?)
      pool = @connection_pools[pool_key] ||= create_pool unless pool
      pool.shard = active_shard
      pool
    end

    def connections
      @connection_pools.values.map(&:connections).inject([], &:+)
    end

    def connection
      pool = current_pool
      begin
        connection = pool.connection
        connection.instance_variable_set(:@schema_cache, @schema_cache)
        connection
      rescue ConnectionError
        raise if active_shard.database_server == Shard.default.database_server && active_shackles_environment == :master
        configs = active_shard.database_server.config(active_shackles_environment)
        raise unless configs.is_a?(Array)
        configs.each_with_index do |config, idx|
          pool = create_pool(config.dup)
          begin
            connection = pool.connection
            connection.instance_variable_set(:@schema_cache, @schema_cache)
          rescue ConnectionError
            raise if idx == configs.length - 1
            next
          end
          @connection_pools[pool_key] = pool
          break connection
        end
      end
    end

    %w{release_connection disconnect! clear_reloadable_connections! verify_active_connections! clear_stale_cached_connections!}.each do |method|
      class_eval(<<-EOS)
          def #{method}
            @connection_pools.values.each(&:#{method})
          end
      EOS
    end

    def clear_idle_connections!(since_when)
      @connection_pools.values.each { |pool| pool.clear_idle_connections!(since_when) }
    end

    protected

    def pool_key
      [active_shackles_environment,
        active_shard.database_server.shareable? ? active_shard.database_server.pool_key : active_shard]
    end

    def create_pool(config = nil)
      shard = active_shard
      unless config
        if shard != Shard.default
          config = shard.database_server.config(active_shackles_environment)
          config = config.first if config.is_a?(Array)
          config = config.dup
        else
          # we read @config instead of calling config so that we get the config
          # *before* %{shard_name} is applied
          # also, we can't just read the database server's config, because
          # different models could be using different configs on the default
          # shard, and database server wouldn't know about that
          config = default_pool.spec.instance_variable_get(:@config)
          if config[active_shackles_environment].is_a?(Hash)
            config = config.merge(config[active_shackles_environment])
          elsif config[active_shackles_environment].is_a?(Array)
            config = config.merge(config[active_shackles_environment].first)
          else
            config = config.dup
          end
        end
      end
      klass = ::Rails.version < '4' ? ::ActiveRecord::Base : ::ActiveRecord::ConnectionAdapters
      spec = klass::ConnectionSpecification.new(config, "#{config[:adapter]}_connection")
      # unfortunately the AR code that does this require logic can't really be
      # called in isolation
      require "active_record/connection_adapters/#{config[:adapter]}_adapter"

      ::ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec).tap do |pool|
        pool.shard = shard
      end
    end
  end
end

