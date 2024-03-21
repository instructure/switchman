# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module PendingMigrationConnection
      module ClassMethods
        def current_role
          ::ActiveRecord::Base.current_role
        end

        def current_switchman_shard
          ::ActiveRecord::Base.current_switchman_shard
        end
      end
    end
  end
end
