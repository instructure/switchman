module Switchman
  module ActiveRecord
    module Base
      module ClassMethods
        delegate :shard, to: :all

        if ::Rails.version < '5'
          def shard_category
            @shard_category || (self.superclass < ::ActiveRecord::Base && self.superclass.shard_category) || :primary
          end
        else
          def shard_category
            connection_specification_name.to_sym
          end
        end

        def shard_category=(category)
          categories = Shard.const_get(:CATEGORIES)
          if categories[shard_category]
            categories[shard_category].delete(self)
            categories.delete(shard_category) if categories[shard_category].empty?
          end
          categories[category] ||= []
          categories[category] << self
          if ::Rails.version < '5'
            connection_handler.uninitialize_ar(self)
            @shard_category = category
            connection_handler.initialize_categories(superclass)
          else
            self.connection_specification_name = category.to_s
          end
        end

        def integral_id?
          if @integral_id == nil
            @integral_id = columns_hash[primary_key]&.type == :integer
          end
          @integral_id
        end

        def transaction(*args)
          if self != ::ActiveRecord::Base && current_scope
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

        def reset_column_information
          @sharded_column_values = {}
          super
        end

        def unscoped
          if block_given?
            super do
              current_scope.shard_value = nil
              yield
            end
          else
            result = super
            result.shard_value = nil
            result
          end
        end
      end

      def self.included(klass)
        klass.extend(ClassMethods)
        klass.set_callback(:initialize, :before) do
          unless @shard
            if self.class.sharded_primary_key?
              @shard = Shard.shard_for(self[self.class.primary_key], Shard.current(self.class.shard_category))
            else
              @shard = Shard.current(self.class.shard_category)
            end
          end
        end
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

      def scope_class
        self.class.base_class
      end

      def save(*args)
        @shard_set_in_stone = true
        scope_class.shard(shard, :implicit).scoping { super }
      end

      def save!(*args)
        @shard_set_in_stone = true
        scope_class.shard(shard, :implicit).scoping { super }
      end

      def destroy
        scope_class.shard(shard, :implicit).scoping { super }
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
        self.class.sharded_primary_key? ? Shard.global_id_for(id).hash : super
      end

      def to_param
        short_id = Shard.short_id_for(id)
        short_id && short_id.to_s
      end

      def initialize_dup(*args)
        copy = super
        @shard_set_in_stone = false
        copy
      end
    end
  end
end
