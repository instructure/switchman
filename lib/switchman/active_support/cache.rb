module Switchman
  module ActiveSupport
    module Cache
      module ClassMethods
        def lookup_store(*store_options)
          store = super
          # can't use defined?, because it's a _ruby_ autoloaded constant,
          # so just checking that will cause it to get required
          if store.class.name == "ActiveSupport::Cache::RedisCacheStore" && !::ActiveSupport::Cache::RedisCacheStore.ancestors.include?(RedisCacheStore)
            ::ActiveSupport::Cache::RedisCacheStore.prepend(RedisCacheStore)
          end
          store.options[:namespace] ||= lambda { Shard.current.default? ? nil : "shard_#{Shard.current.id}" }
          store
        end
      end

      module RedisCacheStore
        def clear(options = {})
          # RedisCacheStore tries to be smart and only clear the cache under your namespace, if you have one set
          # unfortunately, it uses the keys command, which is extraordinarily inefficient in a large redis instance
          # fortunately, we can assume we control the entire instance, because we set up the namespacing, so just
          # always unset it temporarily for clear calls
          options[:namespace] = nil
          super
        end
      end
    end
  end
end
