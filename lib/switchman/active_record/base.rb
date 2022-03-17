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

          Shard.send(:add_sharded_model, self)
        end

        def integral_id?
          @integral_id = columns_hash[primary_key]&.type == :integer if @integral_id.nil?
          @integral_id
        end

        def transaction(**)
          if self != ::ActiveRecord::Base && current_scope
            current_scope.activate do
              db = Shard.current(connection_class_for_self).database_server
              db.unguard { super }
            end
          else
            db = Shard.current(connection_class_for_self).database_server
            db.unguard { super }
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

        def role_overriden?(shard_id)
          current_role(target_shard: shard_id) != current_role(without_overrides: true)
        end

        def establish_connection(config_or_env = nil)
          raise ArgumentError, 'establish connection cannot be used on the non-current shard/role' if config_or_env.is_a?(Symbol) && config_or_env != ::Rails.env.to_sym

          # Ensure we don't randomly surprise change the connection parms associated with a shard/role
          config_or_env = nil if config_or_env == ::Rails.env.to_sym

          config_or_env ||= if current_shard == ::Rails.env.to_sym && current_role == :primary
                              :primary
                            else
                              "#{current_shard}/#{current_role}".to_sym
                            end

          super(config_or_env)
        end

        def connected_to_stack
          return super if ::Rails.version < '7.0' ? Thread.current.thread_variable?(:ar_connected_to_stack) : ::ActiveSupport::IsolatedExecutionState.key?(:active_record_connected_to_stack)

          ret = super
          DatabaseServer.guard_servers
          ret
        end

        # significant change: Allow per-shard roles
        def current_role(without_overrides: false, target_shard: current_shard)
          return super() if without_overrides

          sharded_role = nil
          connected_to_stack.reverse_each do |hash|
            shard_role = hash.dig(:shard_roles, target_shard)
            if shard_role && (hash[:klasses].include?(::ActiveRecord::Base) || hash[:klasses].include?(connection_class_for_self))
              sharded_role = shard_role
              break
            end
          end
          # Allow a shard-specific role to be reverted to regular inheritance
          return sharded_role if sharded_role && sharded_role != :_switchman_inherit

          super()
        end

        # significant change: _don't_ check if klasses.include?(Base)
        # i.e. other sharded models don't inherit the current shard of Base
        def current_shard
          connected_to_stack.reverse_each do |hash|
            return hash[:shard] if hash[:shard] && hash[:klasses].include?(connection_class_for_self)
          end

          default_shard
        end

        def current_switchman_shard
          connected_to_stack.reverse_each do |hash|
            return hash[:switchman_shard] if hash[:switchman_shard] && hash[:klasses].include?(connection_class_for_self)
          end

          Shard.default
        end

        if ::Rails.version < '7.0'
          def connection_class_for_self
            connection_classes
          end
        end
      end

      def self.prepended(klass)
        klass.singleton_class.prepend(ClassMethods)
      end

      def _run_initialize_callbacks
        @shard ||= if self.class.sharded_primary_key?
                     Shard.shard_for(self[self.class.primary_key], Shard.current(self.class.connection_class_for_self))
                   else
                     Shard.current(self.class.connection_class_for_self)
                   end
        super
      end

      def shard
        @shard || Shard.current(self.class.connection_class_for_self) || Shard.default
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
        super
      end

      def save!(*, **)
        @shard_set_in_stone = true
        super
      end

      def destroy
        shard.activate(self.class.connection_class_for_self) { super }
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
        shard.activate(self.class.connection_class_for_self) do
          self.class.transaction(**kwargs, &block)
        end
      end

      def with_transaction_returning_status
        shard.activate(self.class.connection_class_for_self) do
          db = Shard.current(self.class.connection_class_for_self).database_server
          db.unguard { super }
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
        db.unguard { super }
      end

      def id_for_database
        if self.class.sharded_primary_key?
          # It's an int, so so it's safe to just return it without passing it through anything else
          # In theory we should do `@attributes[@primary_key].type.serialize(id)`, but that seems to have surprising side-effects
          id
        else
          super
        end
      end

      protected

      # see also AttributeMethods#connection_class_for_self_code_for_reflection
      def connection_class_for_self_for_reflection(reflection)
        if reflection
          if reflection.options[:polymorphic]
            begin
              read_attribute(reflection.foreign_type)&.constantize&.connection_class_for_self || ::ActiveRecord::Base
            rescue NameError
              # in case someone is abusing foreign_type to not point to an actual class
              ::ActiveRecord::Base
            end
          else
            # otherwise we can just return a symbol for the statically known type of the association
            reflection.klass.connection_class_for_self
          end
        else
          self.class.connection_class_for_self
        end
      end
    end
  end
end
