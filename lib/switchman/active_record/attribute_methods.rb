module Switchman
  module ActiveRecord
    module AttributeMethods
      module ClassMethods

        def sharded_primary_key?
          self != Shard && shard_category != :unsharded && integral_id?
        end

        def sharded_foreign_key?(column_name)
          reflection = reflection_for_integer_attribute(column_name.to_s)
          return false unless reflection
          reflection.options[:polymorphic] || reflection.klass.sharded_primary_key?
        end

        def sharded_column?(column_name)
          column_name = column_name.to_s
          @sharded_column_values ||= {}
          unless @sharded_column_values.has_key?(column_name)
            @sharded_column_values[column_name] = (column_name == primary_key && sharded_primary_key?) || sharded_foreign_key?(column_name)
          end
          @sharded_column_values[column_name]
        end

        protected

        def reflection_for_integer_attribute(attr_name)
          columns_hash[attr_name] && columns_hash[attr_name].type == :integer &&
              reflections.find { |_, r| r.belongs_to? && r.foreign_key == attr_name }.try(:last)
        end

        def define_method_global_attribute(attr_name)
          if sharded_column?(attr_name)
            generated_attribute_methods.module_eval <<-RUBY, __FILE__, __LINE__ + 1
              def __temp__
                Shard.global_id_for(original_#{attr_name}, shard)
              end
              alias_method 'global_#{attr_name}', :__temp__
              undef_method :__temp__
            RUBY
          end
        end

        def define_method_local_attribute(attr_name)
          if sharded_column?(attr_name)
            generated_attribute_methods.module_eval <<-RUBY, __FILE__, __LINE__ + 1
              def __temp__
                Shard.local_id_for(original_#{attr_name}).first
              end
              alias_method 'local_#{attr_name}', :__temp__
              undef_method :__temp__
            RUBY
          end
        end

        def shard_category_code_for_reflection(reflection)
          if reflection
            if reflection.options[:polymorphic]
              # a polymorphic association has to be discovered at runtime. This code ends up being something like
              # context_type.try(:constantize).try(:shard_category) || :default
              "#{reflection.options[:foreign_key_type]}.try(:constantize).try(:shard_category) || :default"
            else
              # otherwise we can just return a symbol for the statically known type of the association
              reflection.klass.shard_category.inspect
            end
          else
            shard_category.inspect
          end
        end

        def define_method_original_attribute(attr_name)
          if sharded_column?(attr_name)
            reflection = reflection_for_integer_attribute(attr_name)
            generated_attribute_methods.module_eval <<-RUBY, __FILE__, __LINE__ + 1
              # rename the original method to original_
              alias_method 'original_#{attr_name}', '#{attr_name}'
              # and replace with one that transposes the id
              def __temp__
                Shard.relative_id_for(original_#{attr_name}, shard, Shard.current(#{shard_category_code_for_reflection(reflection)}))
              end
              alias_method '#{attr_name}', :__temp__
              undef_method :__temp__

              alias_method 'original_#{attr_name}=', '#{attr_name}='
              def __temp__(new_value)
                self.original_#{attr_name} = Shard.relative_id_for(new_value, Shard.current(#{shard_category_code_for_reflection(reflection)}), shard)
              end
              alias_method '#{attr_name}=', :__temp__
              undef_method :__temp__
            RUBY
          end
        end
      end

      def self.included(klass)
        klass.extend(ClassMethods)
        klass.attribute_method_prefix "global_", "local_", "original_"
      end
    end
  end
end
