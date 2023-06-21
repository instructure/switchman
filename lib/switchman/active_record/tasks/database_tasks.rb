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

        def raise_for_multi_db(*)
          # ignore; Switchman doesn't use namespaced tasks for multiple shards; it uses
          # environment variables to filter which shards you want to target
        end
      end
    end
  end
end
