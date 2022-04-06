# frozen_string_literal: true

module Switchman
  module Rails
    module ClassMethods
      def self.prepended(klass)
        # we want to make sure no one tries to assign to Rails.cache,
        # because it would be wrong w.r.t. sharding.
        klass.send(:remove_method, :cache=)
      end

      def cache
        Switchman::Shard.current.database_server.cache_store
      end
    end
  end
end
