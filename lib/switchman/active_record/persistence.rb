# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Persistence
      # touch reads the id attribute directly, so it's not relative to the current shard
      def touch(*, **)
        shard.activate(self.class.connection_class_for_self) { super }
      end

      def update_columns(*)
        shard.activate(self.class.connection_class_for_self) { super }
      end

      def delete
        db = shard.database_server
        db.unguard { super }
      end

      def reload(*)
        res = super
        # When a shadow record is reloaded the real record is returned. So
        # we need to ensure the loaded_from_shard is set correctly after a reload.
        @loaded_from_shard = @shard
        res
      end
    end
  end
end
