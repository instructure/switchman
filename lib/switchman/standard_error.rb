# frozen_string_literal: true

module Switchman
  module StandardError
    def initialize(*args)
      # Shard.current can throw this when switchman isn't working right; if we try to
      # do our stuff here, it'll cause a SystemStackError, which is a pain to deal with
      return super if self.is_a?(::ActiveRecord::ConnectionNotEstablished)

      @active_shards = Shard.sharded_models.map do |klass|
        [klass, Shard.current(klass)]
      end.compact.to_h
      super
    end

    def current_shard(klass = ::ActiveRecord::Base)
      @active_shards&.[](klass) || Shard.default
    end
  end
end
