module Switchman
  module ActiveRecord
    module Base
      module ClassMethods
        attr_writer :shard_category
        delegate :shard, :to => :scoped

        def shard_category
          @shard_category || :default
        end

        def shard_category=(category)
          categories = Shard.const_get(:CATEGORIES)
          if categories[shard_category]
            categories[shard_category].delete(self)
            categories.delete(shard_category) if categories[shard_category].empty?
          end
          # TODO: de-initialize the proxy
          categories[category] ||= []
          categories[category] << self
          @shard_category = category
          connection_handler.initialize_categories(superclass)
        end

        def integral_id?
          if @integral_id == nil
            @integral_id = columns_hash[primary_key].type == :integer
          end
          @integral_id
        end

        def transaction(*args)
          if current_scope
            current_scope.activate do
              db = Shard.current(shard_category).database_server
              if ::Shackles.environment != db.shackles_environment
                db.unshackle { super }
              else
                super
              end
            end
          else
            db = Shard.current(shard_category).database_server
            if ::Shackles.environment != db.shackles_environment
              db.unshackle { super }
            else
              super
            end
          end
        end
      end

      def self.included(klass)
        klass.extend(ClassMethods)
        klass.set_callback(:initialize, :before) { @shard = Shard.current(self.class.shard_category) }
      end

      def shard
        @shard || Shard.current(self.class.shard_category) || Shard.default
      end

      def shard=(new_shard)
        raise ::ActiveRecord::ReadOnlyRecord if !self.new_record? || @shard_set_in_stone
        if shard != new_shard
          attributes.each do |attr, value|
            self[attr] = Shard.relative_id_for(value, shard, new_shard) if self.class.sharded_column?(attr)
          end
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

      def transaction(options={}, &block)
        shard.activate(self.class.shard_category) do
          self.class.transaction(options, &block)
        end
      end

      def hash
        global_id.hash
      end

      def to_param
        Shard.short_id_for(self.id).to_s if persisted?
      end

      def initialize_dup(*args)
        copy = super
        @shard_set_in_stone = false
        copy
      end
    end
  end
end
