# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Persistence
      # touch reads the id attribute directly, so it's not relative to the current shard
      def touch(*, **)
        shard.activate(self.class.connection_classes) { super }
      end

      def update_columns(*)
        shard.activate(self.class.connection_classes) { super }
      end

      def delete
        db = shard.database_server
        return db.unguard { super } unless ::GuardRail.environment == db.guard_rail_environment

        super
      end
    end
  end
end
