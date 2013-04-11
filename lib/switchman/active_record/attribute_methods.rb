module Switchman
  module ActiveRecord
    module AttributeMethods
      module ClassMethods
        protected
        def define_method_global_attribute(attr_name)
          if attr_name == primary_key && integral_id? && self != Shard
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
          if attr_name == primary_key && integral_id? && self != Shard
            generated_attribute_methods.module_eval <<-RUBY, __FILE__, __LINE__ + 1
              def __temp__
                Shard.local_id_for(original_#{attr_name}).first
              end
              alias_method 'local_#{attr_name}', :__temp__
              undef_method :__temp__
            RUBY
          end
        end

        def define_method_original_attribute(attr_name)
          if attr_name == primary_key && integral_id? && self != Shard
            generated_attribute_methods.module_eval <<-RUBY, __FILE__, __LINE__ + 1
              # rename the original method to original_
              alias_method 'original_#{attr_name}', '#{attr_name}'
              # and replace with one that transposes the id
              def __temp__
                Shard.relative_id_for(local_#{attr_name}, shard, Shard.current(self.class.shard_category))
              end
              alias_method '#{attr_name}', :__temp__
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
