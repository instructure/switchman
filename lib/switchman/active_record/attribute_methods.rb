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

        def define_attribute_methods
          super
          # ensure that we're using the sharded attribute method
          # and not the silly one in AR::AttributeMethods::PrimaryKey
          return unless sharded_column?(@primary_key)

          class_eval(build_sharded_getter('id', '_read_attribute(@primary_key)', "::#{connection_class_for_self.name}"), __FILE__, __LINE__)
          class_eval(build_sharded_setter('id', @primary_key, "::#{connection_class_for_self.name}"), __FILE__, __LINE__)
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

        # rubocop:disable Naming/MethodParameterName
        def define_cached_method(owner, name, namespace:, as:, &block)
          if ::Rails.version < '7.0'
            yield owner
            owner.rename_method(as, name)
          else
            owner.define_cached_method(name, namespace: namespace, as: as, &block)
          end
        end
        # rubocop:enable Naming/MethodParameterName

        def define_method_global_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            define_cached_method(owner, "global_#{attr_name}", as: "sharded_global_#{attr_name}", namespace: :switchman) do |batch|
              batch << <<-RUBY
                def sharded_global_#{attr_name}
                  raw_value = original_#{attr_name}
                  return nil if raw_value.nil?
                  return raw_value if raw_value > ::Switchman::Shard::IDS_PER_SHARD

                  ::Switchman::Shard.global_id_for(raw_value, shard)
                end
              RUBY
            end
          else
            define_method_unsharded_column(attr_name, 'global', owner)
          end
        end

        def define_method_local_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            define_cached_method(owner, "local_#{attr_name}", as: "sharded_local_#{attr_name}", namespace: :switchman) do |batch|
              batch << <<-RUBY
                def sharded_local_#{attr_name}
                  raw_value = original_#{attr_name}
                  return nil if raw_value.nil?
                  return raw_value % ::Switchman::Shard::IDS_PER_SHARD
                end
              RUBY
            end
          else
            define_method_unsharded_column(attr_name, 'local', owner)
          end
        end

        # see also Base#connection_class_for_self_for_reflection
        # the difference being this will output static strings for the common cases, making them
        # more performant
        def connection_class_for_self_code_for_reflection(reflection)
          if reflection
            if reflection.options[:polymorphic]
              # a polymorphic association has to be discovered at runtime. This code ends up being something like
              # context_type.&.constantize&.connection_class_for_self
              "begin;read_attribute(:#{reflection.foreign_type})&.constantize&.connection_class_for_self;rescue NameError;::ActiveRecord::Base;end"
            else
              # otherwise we can just return a symbol for the statically known type of the association
              "::#{reflection.klass.connection_class_for_self.name}"
            end
          else
            "::#{connection_class_for_self.name}"
          end
        end

        def define_method_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            reflection = reflection_for_integer_attribute(attr_name)
            class_name = connection_class_for_self_code_for_reflection(reflection)
            safe_class_name = class_name.unpack1('h*')
            define_cached_method(owner, attr_name, as: "sharded_#{safe_class_name}_#{attr_name}", namespace: :switchman) do |batch|
              batch << build_sharded_getter("sharded_#{safe_class_name}_#{attr_name}", "original_#{attr_name}", class_name)
            end
          else
            define_cached_method(owner, attr_name, as: "plain_#{attr_name}", namespace: :switchman) do |batch|
              batch << <<-RUBY
                def plain_#{attr_name}
                  _read_attribute("#{attr_name}") { |n| missing_attribute(n, caller) }
                end
              RUBY
            end
          end
        end

        def build_sharded_getter(attr_name, raw_expr, attr_connection_class)
          <<-RUBY
            def #{attr_name}
              raw_value = #{raw_expr}
              return nil if raw_value.nil?

              abs_raw_value = raw_value.abs
              current_shard = ::Switchman::Shard.current(#{attr_connection_class})
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
          RUBY
        end

        def define_method_attribute=(attr_name, owner:)
          if sharded_column?(attr_name)
            reflection = reflection_for_integer_attribute(attr_name)
            class_name = connection_class_for_self_code_for_reflection(reflection)
            safe_class_name = class_name.unpack1('h*')
            define_cached_method(owner, "#{attr_name}=", as: "sharded_#{safe_class_name}_#{attr_name}=", namespace: :switchman) do |batch|
              batch << build_sharded_setter("sharded_#{safe_class_name}_#{attr_name}", attr_name, class_name)
            end
          else
            define_cached_method(owner, "#{attr_name}=", as: "plain_#{attr_name}=", namespace: :switchman) do |batch|
              batch << <<-RUBY
                def plain_#{attr_name}=(new_value)
                  _write_attribute('#{attr_name}', new_value)
                end
              RUBY
            end
          end
        end

        def build_sharded_setter(attr_name, attr_field, attr_connection_class)
          <<-RUBY
            def #{attr_name}=(new_value)
              self.original_#{attr_field} = ::Switchman::Shard.relative_id_for(new_value, ::Switchman::Shard.current(#{attr_connection_class}), shard)
            end
          RUBY
        end

        def define_method_original_attribute(attr_name, owner:)
          if sharded_column?(attr_name)
            define_cached_method(owner, "original_#{attr_name}", as: "sharded_original_#{attr_name}", namespace: :switchman) do |batch|
              batch << <<-RUBY
                def sharded_original_#{attr_name}
                  _read_attribute("#{attr_name}") { |n| missing_attribute(n, caller) }
                end
              RUBY
            end
          else
            define_method_unsharded_column(attr_name, 'global', owner)
          end
        end

        def define_method_original_attribute=(attr_name, owner:)
          return unless sharded_column?(attr_name)

          define_cached_method(owner, "original_#{attr_name}=", as: "sharded_original_#{attr_name}=", namespace: :switchman) do |batch|
            batch << <<-RUBY
              def sharded_original_#{attr_name}=(new_value)
                _write_attribute('#{attr_name}', new_value)
              end
            RUBY
          end
        end

        def define_method_unsharded_column(attr_name, prefix, owner)
          return if columns_hash["#{prefix}_#{attr_name}"] || attr_name == 'id'

          define_cached_method(owner, "#{prefix}_#{attr_name}", as: "unsharded_#{prefix}_#{attr_name}", namespace: :switchman) do |batch|
            batch << <<-RUBY
              def unsharded_#{prefix}_#{attr_name}
                raise NoMethodError, "undefined method `#{prefix}_#{attr_name}'; are you missing an association?"
              end
            RUBY
          end
        end
      end

      def self.prepended(klass)
        klass.singleton_class.prepend(ClassMethods)
        klass.attribute_method_prefix 'global_', 'local_', 'original_'
        klass.attribute_method_affix prefix: 'original_', suffix: '='
      end

      # these are called if the specific methods haven't been defined yet
      def attribute(attr_name)
        return super unless self.class.sharded_column?(attr_name)

        reflection = self.class.send(:reflection_for_integer_attribute, attr_name)
        ::Switchman::Shard.relative_id_for(super, shard, ::Switchman::Shard.current(connection_class_for_self_for_reflection(reflection)))
      end

      def attribute=(attr_name, new_value)
        unless self.class.sharded_column?(attr_name)
          super
          return
        end

        reflection = self.class.send(:reflection_for_integer_attribute, attr_name)
        super(::Switchman::Shard.relative_id_for(new_value, ::Switchman::Shard.current(connection_class_for_self_for_reflection(reflection)), shard))
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
          ::Switchman::Shard.local_id_for(attribute(attr_name)).first
        else
          attribute(attr_name)
        end
      end
    end
  end
end
