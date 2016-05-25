module Switchman
  class ShardAwareStatementCache

    attr_accessor :__cache, :shard_category
    private :__cache, :shard_category

    def initialize(shard_category)
      self.extend Mutex_m
      self.__cache = {}
      self.shard_category = shard_category
    end

    def [](key)
      __cache[[key, Shard.current(shard_category).id]]
    end

    def []=(key, value)
      __cache[[key, Shard.current(shard_category).id]] = value
    end
  end
end
