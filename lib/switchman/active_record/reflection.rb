# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Reflection
      module AbstractReflection
        def shard(owner)
          if polymorphic? || klass.connection_class_for_self == owner.class.connection_class_for_self
            # polymorphic associations assume the same shard as the owning item
            owner.shard
          else
            Shard.default
          end
        end
      end

      module AssociationScopeCache
        def initialize(*args)
          super
          # on ThroughReflection, these won't be initialized (cause it doesn't
          # inherit from AssociationReflection), so make sure they're
          # initialized here
          @association_scope_cache ||= {}
          @scope_lock ||= Mutex.new
        end

        # cache association scopes by shard.
        # this technically belongs on AssociationReflection, but we put it on
        # ThroughReflection as well, instead of delegating to its internal
        # HasManyAssociation, losing its proper `klass`
        def association_scope_cache(klass, owner, &)
          key = self
          key = [key, owner._read_attribute(@foreign_type)] if polymorphic?
          key = [key, shard(owner).id].flatten

          if ::Rails.version < "7.2"
            klass.cached_find_by_statement(key, &)
          else
            klass.with_connection do |connection|
              klass.cached_find_by_statement(connection, key, &)
            end
          end
        end
      end

      module AssociationReflection
        def join_id_for(owner)
          owner.send(join_foreign_key) # use sharded id values in association binds
        end
      end
    end
  end
end
