# frozen_string_literal: true

module Switchman
  module ActionController
    module Caching
      module ConfigMethods
        # always go through Rails.cache, which will give you the cache store
        # appropriate to the current shard.
        def cache_store
          ::Rails.cache
        end

        # disallow assigning to ActionController::Base.cache_store or
        # ActionController::Base#cache_store for the same reasons we disallow
        # assigning to Rails.cache
        def cache_store=(cache)
          raise NoMethodError
        end
      end

      include ConfigMethods

      def self.included(base)
        base.singleton_class.prepend(ConfigMethods)
      end
    end
  end
end
