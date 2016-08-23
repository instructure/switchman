module Switchman
  module StandardError
    def initialize(*args)
      @active_shards = Shard.send(:active_shards).dup
      super
    end

    def current_shard(category = :default)
      @active_shards[category] || Shard.default
    end
  end
end
