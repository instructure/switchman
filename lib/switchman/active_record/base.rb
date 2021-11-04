# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Base
      module ClassMethods
        delegate :shard, to: :all

        def find_ids_in_ranges(opts = {}, &block)
          opts.reverse_merge!(loose: true)
          all.find_ids_in_ranges(opts, &block)
        end

        def sharded_model
          self.abstract_class = true

          return if self == UnshardedRecord

          Shard.send(:add_sharded_model, self)
        end

        def integral_id?
          @integral_id = columns_hash[primary_key]&.type == :integer if @integral_id.nil?
          @integral_id
        end

        def transaction(**)
          if self != ::ActiveRecord::Base && current_scope
            current_scope.activate do
              db = Shard.current(connection_classes).database_server
              if ::GuardRail.environment == db.guard_rail_environment
                super
              else
                db.unguard { super }
              end
            end
          else
            db = Shard.current(connection_classes).database_server
            if ::GuardRail.environment == db.guard_rail_environment
              super
            else
              db.unguard { super }
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
          ::ActiveRecord::Base.connection_handler.connection_pool_list.each do |pool|
            pool.connection(switch_shard: false).clear_query_cache if pool.active_connection?
          end
        end

        # significant change: _don't_ check if klasses.include?(Base)
        # i.e. other sharded models don't inherit the current shard of Base
        def current_shard
          connected_to_stack.reverse_each do |hash|
            return hash[:shard] if hash[:shard] && hash[:klasses].include?(connection_classes)
          end

          default_shard
        end

        def current_switchman_shard
          connected_to_stack.reverse_each do |hash|
            return hash[:switchman_shard] if hash[:switchman_shard] && hash[:klasses].include?(connection_classes)
          end

          Shard.default
        end
      end

      def self.prepended(klass)
        klass.singleton_class.prepend(ClassMethods)
      end

      def _run_initialize_callbacks
        @shard ||= if self.class.sharded_primary_key?
                     Shard.shard_for(self[self.class.primary_key], Shard.current(self.class.connection_classes))
                   else
                     Shard.current(self.class.connection_classes)
                   end
        super
      end

      def shard
        @shard || Shard.current(self.class.connection_classes) || Shard.default
      end

      def shard=(new_shard)
        raise ::ActiveRecord::ReadOnlyRecord if !new_record? || @shard_set_in_stone

        return if shard == new_shard

        attributes.each do |attr, value|
          self[attr] = Shard.relative_id_for(value, shard, new_shard) if self.class.sharded_column?(attr)
        end
        @shard = new_shard
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
        shard.activate(self.class.connection_classes) { super }
      end

      def clone
        result = super
        # TODO: adjust foreign keys
        # don't use the setter, cause the foreign keys are already
        # relative to this shard
        result.instance_variable_set(:@shard, shard)
        result
      end

      def transaction(**kwargs, &block)
        shard.activate(self.class.connection_classes) do
          self.class.transaction(**kwargs, &block)
        end
      end

      def with_transaction_returning_status
        shard.activate(self.class.connection_classes) do
          super
        end
      end

      def hash
        self.class.sharded_primary_key? ? self.class.hash ^ global_id.hash : super
      end

      def to_param
        short_id = Shard.short_id_for(id)
        short_id&.to_s
      end

      def initialize_dup(*args)
        copy = super
        @shard_set_in_stone = false
        copy
      end

      def update_columns(*)
        db = shard.database_server
        return db.unguard { super } if ::GuardRail.environment != db.guard_rail_environment

        super
      end

      def id_for_database
        if self.class.sharded_primary_key?
          @attributes[@primary_key].type.serialize(id)
        else
          super
        end
      end

      protected

      # see also AttributeMethods#connection_classes_code_for_reflection
      def connection_classes_for_reflection(reflection)
        if reflection
          if reflection.options[:polymorphic]
            begin
              read_attribute(reflection.foreign_type)&.constantize&.connection_classes
            rescue NameError
              # in case someone is abusing foreign_type to not point to an actual class
              ::ActiveRecord::Base
            end
          else
            # otherwise we can just return a symbol for the statically known type of the association
            reflection.klass.connection_classes
          end
        else
          self.class.connection_classes
        end
      end
    end
  end
end
