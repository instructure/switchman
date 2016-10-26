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

      module AssociationReflection
        # removes memoization - ActiveRecord::ModelSchema does that anyway;
        # and in fact this is the exact change AR makes in 4.2+
        if ::Rails.version < '4.2'
          def quoted_table_name
            klass.quoted_table_name
          end
        else
          def join_id_for(owner)
            owner.send(active_record_primary_key) # use sharded id values in association binds
          end
        end

        # cache association scopes by shard.
        if ::Rails.version >= '4.2'
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
      end
    end
  end
end
