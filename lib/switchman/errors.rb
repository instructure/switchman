# frozen_string_literal: true

module Switchman
  module Errors
    class NonExistentShardError < RuntimeError; end

    class ParallelShardExecError < RuntimeError; end
  end
end
