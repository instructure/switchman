# frozen_string_literal: true

module Switchman
  module Errors
    class ManuallyCreatedShadowRecordError < RuntimeError
      DEFAULT_MSG = "It looks like you're trying to manually create a shadow record. " \
                    "Please use Switchman::ActiveRecord::Base#save_shadow_record instead."

      def initialize(msg = DEFAULT_MSG)
        super
      end
    end

    class NonExistentShardError < RuntimeError; end

    class ParallelShardExecError < RuntimeError; end

    class ShadowRecordError < RuntimeError; end

    class UnshardedTableError < RuntimeError; end
  end
end
