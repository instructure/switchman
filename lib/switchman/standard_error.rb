# frozen_string_literal: true

module Switchman
  module StandardError
    def initialize(*args)
      super
      # These seem to get themselves into a bad state if we try to lookup shards while processing
      return if is_a?(IO::EAGAINWaitReadable)

      return if Thread.current[:switchman_error_handler]

      begin
        Thread.current[:switchman_error_handler] = true

        begin
          @active_shards ||= Shard.active_shards if defined?(Shard)
        rescue
          # If we hit an error really early in boot, activerecord may not be initialized yet
        end
      ensure
        Thread.current[:switchman_error_handler] = nil
      end
    end

    def current_shard(klass = ::ActiveRecord::Base)
      @active_shards&.[](klass) || Shard.default
    end
  end
end
