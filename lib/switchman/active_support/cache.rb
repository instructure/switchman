module Switchman
  module ActiveSupport
    module Cache
      module Store
        def initialize_with_sharding(options = nil)
          options ||= {}
          options[:namespace] ||= lambda { Shard.current.default? ? nil : "shard_#{Shard.current.id}" }
          initialize_without_sharding(options)
        end

        def self.included(klass)
          klass.alias_method_chain(:initialize, :sharding) unless klass.private_instance_methods.include?(:initialize_without_sharding)
        end
      end
    end
  end
end
