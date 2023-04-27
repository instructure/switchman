# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Tasks
      module DatabaseTasks
        def drop(*)
          super
          # no really, it's gone
          Switchman.cache.delete("default_shard")
          Shard.default(reload: true)
        end
      end
    end
  end
end
