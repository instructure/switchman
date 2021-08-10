# frozen_string_literal: true

module Switchman
  module StandardError
    def initialize(*args)
      # Shard.current can throw this when switchman isn't working right; if we try to
      # do our stuff here, it'll cause a SystemStackError, which is a pain to deal with
      if is_a?(::ActiveRecord::ConnectionNotEstablished)
        super
        return
      end

      @active_shards = Shard.active_shards if defined?(Shard)

      super
    end

    def current_shard(klass = ::ActiveRecord::Base)
      @active_shards&.[](klass) || Shard.default
    end
  end
end
