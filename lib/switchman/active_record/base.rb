# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Base
      module ClassMethods
        delegate :shard, to: :all

        def find_ids_in_ranges(opts={}, &block)
          opts.reverse_merge!(:loose => true)
          all.find_ids_in_ranges(opts, &block)
        end

        def shard_category
          connection_specification_name.to_sym
        end

        def shard_category=(category)
          categories = Shard.const_get(:CATEGORIES)
          if categories[shard_category]
            categories[shard_category].delete(self)
            categories.delete(shard_category) if categories[shard_category].empty?
          end
          categories[category] ||= []
          categories[category] << self
          self.connection_specification_name = category.to_s
        end

        def integral_id?
          if @integral_id == nil
            @integral_id = columns_hash[primary_key]&.type == :integer
          end
          @integral_id
        end

        def transaction(**)
          if self != ::ActiveRecord::Base && current_scope
            current_scope.activate do
              db = Shard.current(shard_category).database_server
              if ::GuardRail.environment != db.guard_rail_environment
                db.unguard { super }
              else
                super
              end
            end
          else
            db = Shard.current(shard_category).database_server
            if ::GuardRail.environment != db.guard_rail_environment
              db.unguard { super }
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

        def clear_query_caches_for_current_thread
          ::ActiveRecord::Base.connection_handlers.each_value do |handler|
            handler.connection_pool_list.each do |pool|
              pool.connection(switch_shard: false).clear_query_cache if pool.active_connection?
            end
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

      def save(*, **)
        @shard_set_in_stone = true
        (self.class.current_scope || self.class.default_scoped).shard(shard, :implicit).scoping { super }
      end

      def save!(*, **)
        @shard_set_in_stone = true
        (self.class.current_scope || self.class.default_scoped).shard(shard, :implicit).scoping { super }
      end

      def destroy
        self.class.shard(shard, :implicit).scoping { super }
      end

      def clone
        result = super
        # TODO: adjust foreign keys
        # don't use the setter, cause the foreign keys are already
        # relative to this shard
        result.instance_variable_set(:@shard, self.shard)
        result
      end

      def transaction(**kwargs, &block)
        shard.activate(self.class.shard_category) do
          self.class.transaction(**kwargs, &block)
        end
      end

      def hash
        self.class.sharded_primary_key? ? self.class.hash ^ Shard.global_id_for(id).hash : super
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

      def quoted_id
        return super unless self.class.sharded_primary_key?
        # do this the Rails 4.2 way, so that if Shard.current != self.shard, the id gets transposed
        self.class.connection.quote(id)
      end

      def update_columns(*)
        db = Shard.current(self.class.shard_category).database_server
        if ::GuardRail.environment != db.guard_rail_environment
          return db.unguard { super }
        else
          super
        end
      end

      protected

      # see also AttributeMethods#shard_category_code_for_reflection
      def shard_category_for_reflection(reflection)
        if reflection
          if reflection.options[:polymorphic]
            begin
              read_attribute(reflection.foreign_type)&.constantize&.shard_category || :primary
            rescue NameError
              # in case someone is abusing foreign_type to not point to an actual class
              :primary
            end
          else
            # otherwise we can just return a symbol for the statically known type of the association
            reflection.klass.shard_category
          end
        else
          shard_category
        end
      end
    end
  end
end
