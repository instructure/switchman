module Switchman
  class NonExistentShardError < RuntimeError; end
  class ParallelShardExecError < RuntimeError; end
end
