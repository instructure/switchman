module Switchman
  module ActiveRecord
    module Reflection
      module AbstractReflection
        def shard(owner)
          if polymorphic? || klass.shard_category == owner.class.shard_category
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
        def association_scope_cache(conn, owner)
          key = conn.prepared_statements
          if polymorphic?
            key = [key, owner._read_attribute(@foreign_type)]
          end
          key = [key, shard(owner).id].flatten
          @association_scope_cache[key] ||= @scope_lock.synchronize {
            @association_scope_cache[key] ||= yield
          }
        end
      end

      module AssociationReflection
        def join_id_for(owner)
          owner.send(active_record_primary_key) # use sharded id values in association binds
        end
      end
    end
  end
end
