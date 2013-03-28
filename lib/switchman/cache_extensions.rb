module Switchman::CacheExtensions
  module ClassMethods
    def cache_with_sharding
      Switchman::Shard.current.database_server.cache_store
    end
  end

  def self.included(klass)
    klass.extend(ClassMethods)
    klass.singleton_class.alias_method_chain :cache, :sharding
  end
end
