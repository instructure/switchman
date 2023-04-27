# frozen_string_literal: true

module Switchman
  module ActiveSupport
    module Cache
      module ClassMethods
        def lookup_stores(cache_store_config)
          result = {}
          cache_store_config.each do |key, value|
            next if value.is_a?(String)

            result[key] = ::ActiveSupport::Cache.lookup_store(value)
          end

          cache_store_config.each do |key, value| # rubocop:disable Style/CombinableLoops
            next unless value.is_a?(String)

            result[key] = result[value]
          end
          result
        end

        def lookup_store(*store_options)
          store = super
          # can't use defined?, because it's a _ruby_ autoloaded constant,
          # so just checking that will cause it to get required
          if store.instance_of?(ActiveSupport::Cache::RedisCacheStore) &&
             !::ActiveSupport::Cache::RedisCacheStore <= RedisCacheStore
            ::ActiveSupport::Cache::RedisCacheStore.prepend(RedisCacheStore)
          end
          store.options[:namespace] ||= -> { Shard.current.default? ? nil : "shard_#{Shard.current.id}" }
          store
        end
      end

      module RedisCacheStore
        def clear(namespace: nil, **)
          # RedisCacheStore tries to be smart and only clear the cache under your namespace, if you have one set
          # unfortunately, it uses the keys command, which is extraordinarily inefficient in a large redis instance
          # fortunately, we can assume we control the entire instance, because we set up the namespacing, so just
          # always unset it temporarily for clear calls
          namespace = nil # rubocop:disable Lint/ShadowedArgument
          super
        end
      end
    end
  end
end
