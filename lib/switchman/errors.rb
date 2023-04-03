# frozen_string_literal: true

module Switchman
  module Errors
    class ManuallyCreatedShadowRecordError < RuntimeError
      def initialize(msg = "It looks like you're trying to manually create a shadow record. Please use Switchman::ActiveRecord::Base#save_shadow_record instead.")
        super
      end
    end

    class NonExistentShardError < RuntimeError; end

    class ParallelShardExecError < RuntimeError; end

    class ShadowRecordError < RuntimeError; end

    class UnshardedTableError < RuntimeError; end
  end
end
