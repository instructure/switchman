module Switchman
  module ActiveSupport
    module Cache
      module ClassMethods
        def lookup_store_with_sharding(*store_options)
          store = lookup_store_without_sharding(*store_options)
          store.options[:namespace] ||= lambda { Shard.current.default? ? nil : "shard_#{Shard.current.id}" }
          store
        end
      end

      def self.included(klass)
        klass.extend(ClassMethods)
        klass.singleton_class.alias_method_chain(:lookup_store, :sharding)
      end
    end
  end
end
