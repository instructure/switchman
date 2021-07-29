# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module AttributeMethods
      module ClassMethods
        def sharded_primary_key?
          !(self <= UnshardedRecord) && integral_id?
        end

        def sharded_foreign_key?(column_name)
          reflection = reflection_for_integer_attribute(column_name.to_s)
          return false unless reflection

          reflection.options[:polymorphic] || reflection.klass.sharded_primary_key?
        end

        def sharded_column?(column_name)
          column_name = column_name.to_s
          @sharded_column_values ||= {}
          unless @sharded_column_values.key?(column_name)
            @sharded_column_values[column_name] =
              (column_name == primary_key && sharded_primary_key?) || sharded_foreign_key?(column_name)
          end
          @sharded_column_values[column_name]
        end

        protected

        def reflection_for_integer_attribute(attr_name)
          attr_name = attr_name.to_s
          columns_hash[attr_name] && columns_hash[attr_name].type == :integer &&
            reflections.find { |_, r| r.belongs_to? && r.foreign_key.to_s == attr_name }&.last
        rescue ::ActiveRecord::StatementInvalid
          # this is for when models are referenced in initializers before migrations have been run
          raise if connection.open_transactions.positive?
        end

        def define_method_global_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            owner << <<-RUBY
              def global_#{attr_name}
                raw_value = original_#{attr_name}
                return nil if raw_value.nil?
                return raw_value if raw_value > ::Switchman::Shard::IDS_PER_SHARD

                ::Switchman::Shard.global_id_for(raw_value, shard)
              end
            RUBY
          else
            define_method_unsharded_column(attr_name, 'global', owner)
          end
        end

        def define_method_local_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            owner << <<-RUBY
              def local_#{attr_name}
                raw_value = original_#{attr_name}
                return nil if raw_value.nil?
                return raw_value % ::Switchman::Shard::IDS_PER_SHARD
              end
            RUBY
          else
            define_method_unsharded_column(attr_name, 'local', owner)
          end
        end

        # see also Base#connection_classes_for_reflection
        # the difference being this will output static strings for the common cases, making them
        # more performant
        def connection_classes_code_for_reflection(reflection)
          if reflection
            if reflection.options[:polymorphic]
              # a polymorphic association has to be discovered at runtime. This code ends up being something like
              # context_type.&.constantize&.connection_classes
              "read_attribute(:#{reflection.foreign_type})&.constantize&.connection_classes"
            else
              # otherwise we can just return a symbol for the statically known type of the association
              "::#{reflection.klass.connection_classes.name}"
            end
          else
            "::#{connection_classes.name}"
          end
        end

        # just a dummy class with the proper interface that calls module_eval immediately
        class CodeGenerator
          def initialize(mod, line)
            @module = mod
            @line = line
          end

          def <<(string)
            @module.module_eval(string, __FILE__, @line)
          end
        end

        def define_method_original_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            reflection = reflection_for_integer_attribute(attr_name)
            if attr_name == 'id'
              return if method_defined?(:original_id)

              owner = CodeGenerator.new(self, __LINE__ + 4)
            end

            owner << <<-RUBY
              # rename the original method to original_*
              alias_method 'original_#{attr_name}', '#{attr_name}'
              # and replace with one that transposes the id
              def #{attr_name}
                raw_value = original_#{attr_name}
                return nil if raw_value.nil?

                abs_raw_value = raw_value.abs
                current_shard = ::Switchman::Shard.current(#{connection_classes_code_for_reflection(reflection)})
                same_shard = shard == current_shard
                return raw_value if same_shard && abs_raw_value < ::Switchman::Shard::IDS_PER_SHARD

                value_shard_id = abs_raw_value / ::Switchman::Shard::IDS_PER_SHARD
                # this is a stupid case when someone stuffed a global id for the current shard in instead
                # of a local id
                return raw_value % ::Switchman::Shard::IDS_PER_SHARD if value_shard_id == current_shard.id
                return raw_value if !same_shard && abs_raw_value > ::Switchman::Shard::IDS_PER_SHARD
                return shard.global_id_for(raw_value) if !same_shard && abs_raw_value < ::Switchman::Shard::IDS_PER_SHARD

                ::Switchman::Shard.relative_id_for(raw_value, shard, current_shard)
              end

              alias_method 'original_#{attr_name}=', '#{attr_name}='
              def #{attr_name}=(new_value)
                self.original_#{attr_name} = ::Switchman::Shard.relative_id_for(new_value, ::Switchman::Shard.current(#{connection_classes_code_for_reflection(reflection)}), shard)
              end
            RUBY
          else
            define_method_unsharded_column(attr_name, 'global', owner)
          end
        end

        def define_method_unsharded_column(attr_name, prefix, owner)
          return if columns_hash["#{prefix}_#{attr_name}"]

          owner << <<-RUBY
            def #{prefix}_#{attr_name}
              raise NoMethodError, "undefined method `#{prefix}_#{attr_name}'; are you missing an association?"
            end
          RUBY
        end
      end

      def self.included(klass)
        klass.singleton_class.include(ClassMethods)
        klass.attribute_method_prefix 'global_', 'local_', 'original_'
      end

      # ensure that we're using the sharded attribute method
      # and not the silly one in AR::AttributeMethods::PrimaryKey
      def id
        return super if is_a?(Shard)

        self.class.define_attribute_methods
        super
      end

      # these are called if the specific methods haven't been defined yet
      def attribute(attr_name)
        return super unless self.class.sharded_column?(attr_name)

        reflection = self.class.send(:reflection_for_integer_attribute, attr_name)
        ::Switchman::Shard.relative_id_for(super, shard, ::Switchman::Shard.current(connection_classes_for_reflection(reflection)))
      end

      def attribute=(attr_name, new_value)
        unless self.class.sharded_column?(attr_name)
          super
          return
        end

        reflection = self.class.send(:reflection_for_integer_attribute, attr_name)
        super(::Switchman::Shard.relative_id_for(new_value, ::Switchman::Shard.current(connection_classes_for_reflection(reflection)), shard))
      end

      def global_attribute(attr_name)
        if self.class.sharded_column?(attr_name)
          ::Switchman::Shard.global_id_for(attribute(attr_name), shard)
        else
          attribute(attr_name)
        end
      end

      def local_attribute(attr_name)
        if self.class.sharded_column?(attr_name)
          ::Switchman::Shard.local_id_for(attribute(attr_name), shard).first
        else
          attribute(attr_name)
        end
      end

      private

      def connection_classes_for_reflection(reflection)
        if reflection
          if reflection.options[:polymorphic]
            read_attribute(reflection.foreign_type)&.constantize&.connection_classes
          else
            reflection.klass.connection_classes
          end
        else
          self.class.connection_classes
        end
      end
    end
  end
end
