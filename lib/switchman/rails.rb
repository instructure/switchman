module Switchman::Rails
  module ClassMethods
    def self.prepended(klass)
      # in Rails 4+, the Rails.cache= method was used during bootstrap to set
      # Rails.cache(_without_sharding) to the value from the config file. but now
      # that that's done (the bootstrap happened before this module is included
      # into Rails), we want to make sure no one tries to assign to Rails.cache,
      # because it would be wrong w.r.t. sharding.
      klass.send(:remove_method, :cache=)
    end

    def cache
      Switchman::Shard.current.database_server.cache_store
    end
  end
end
