module Switchman
  module ActiveRecord
    module Base
      module ClassMethods
        attr_writer :shard_category
        delegate :shard, :to => :scoped

        def shard_category
          @shard_category || :default
        end

        def integral_id?
          if @integral_id == nil
            @integral_id = columns_hash[primary_key].type == :integer
          end
          @integral_id
        end
      end

      def self.included(klass)
        klass.extend(ClassMethods)
        klass.set_callback(:initialize, :before) { @shard = Shard.current(self.class.shard_category) }
      end

      def shard
        @shard || Shard.default
      end

      def shard=(new_shard)
        raise ::ActiveRecord::ReadOnlyRecord if !self.new_record? || @shard_set_in_stone
        if shard != new_shard
          # TODO: adjust foreign keys
          @shard = new_shard
        end
      end

      def save(*args)
        @shard_set_in_stone = true
        shard.activate(self.class.shard_category) { super }
      end

      def save!(*args)
        @shard_set_in_stone = true
        shard.activate(self.class.shard_category) { super }
      end

      def destroy
        shard.activate(self.class.shard_category) { super }
      end

      def clone
        result = super
        # TODO: adjust foreign keys
        # don't use the setter, cause the foreign keys are already
        # relative to this shard
        result.instance_variable_set(:@shard, self.shard)
        result
      end

      def transaction(&block)
        shard.activate do
          super
        end
      end

      def hash
        global_id.hash
      end
    end
  end
end
