# frozen_string_literal: true

module Switchman
  module StandardError
    def initialize(*args)
      @active_shards = Shard.send(:active_shards).dup
      super
    end

    def current_shard(category = :primary)
      @active_shards&.[](category) || Shard.default
    end
  end
end
