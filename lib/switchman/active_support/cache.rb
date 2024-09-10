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
          # must use the string name, otherwise it will try to auto-load the constant
          # and we don't want to require redis in this file (since it's not a hard dependency)
          # rubocop:disable Style/ClassEqualityComparison
          if store.class.name == "ActiveSupport::Cache::RedisCacheStore" &&
             !(::ActiveSupport::Cache::RedisCacheStore <= RedisCacheStore)
            ::ActiveSupport::Cache::RedisCacheStore.prepend(RedisCacheStore)
          end
          # rubocop:enable Style/ClassEqualityComparison
          store.options[:namespace] ||= -> { Shard.current.default? ? nil : "shard_#{Shard.current.id}" }
          store
        end
      end

      module RedisCacheStore
        def clear(namespace: nil, **)
          # RedisCacheStore tries to be smart and only clear the cache under your namespace, if you have one set
          # unfortunately, it doesn't work using redis clustering because of the way redis keys are distributed
          # fortunately, we can assume we control the entire instance, because we set up the namespacing, so just
          # always unset it temporarily for clear calls
          namespace = nil # rubocop:disable Lint/ShadowedArgument
          super
        end
      end
    end
  end
end
