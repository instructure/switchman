# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Persistence
      # touch reads the id attribute directly, so it's not relative to the current shard
      def touch(*, **)
        writable_shadow_record_warning
        shard.activate(self.class.connection_class_for_self) { super }
      end

      def update_columns(*)
        writable_shadow_record_warning
        shard.activate(self.class.connection_class_for_self) { super }
      end

      def delete
        db = shard.database_server
        db.unguard { super }
      end

      def destroy
        writable_shadow_record_warning
        super
      end

      def create_or_update(**, &block)
        writable_shadow_record_warning
        super
      end

      def reload(*)
        res = super
        # When a shadow record is reloaded the real record is returned. So
        # we need to ensure the loaded_from_shard is set correctly after a reload.
        @loaded_from_shard = @shard
        if @readonly_from_shadow
          @readonly_from_shadow = false
          @readonly = false
        end
        res
      end

      def writable_shadow_record_warning
        return unless shadow_record? && Switchman.config[:writable_shadow_records]

        Switchman::Deprecation.warn("writing to shadow records is not supported")
      end
    end
  end
end
