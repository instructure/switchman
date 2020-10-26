# frozen_string_literal: true

module Switchman
  class NonExistentShardError < RuntimeError; end
  class ParallelShardExecError < RuntimeError; end
end
