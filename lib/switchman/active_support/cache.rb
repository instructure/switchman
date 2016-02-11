module Switchman
  module ActiveSupport
    module Cache
      module ClassMethods
        def lookup_store(*store_options)
          store = super
          store.options[:namespace] ||= lambda { Shard.current.default? ? nil : "shard_#{Shard.current.id}" }
          store
        end
      end
    end
  end
end
