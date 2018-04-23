module Switchman
  module ActiveRecord
    module Persistence
      # touch reads the id attribute directly, so it's not relative to the current shard
      def touch(*)
        shard.activate(self.class.shard_category) { super }
      end

      if ::Rails.version >= '5.2'
        def update_columns(*)
          shard.activate(self.class.shard_category) { super }
        end
      end
    end
  end
end