module Switchman
  module ActiveRecord
    module FinderMethods
      def find_one(id)
        if shard_source_value != :implicit
          return self.activate { super(Shard.relative_id_for(id, Shard.current(klass.shard_category), primary_shard)) }
        end

        local_id, shard = Shard.local_id_for(id)
        if shard
          if ::Rails.version < '4.2'
            # find_one uses binds, so we can't depend on QueryMethods
            # catching it
            begin
              old_shard_value = shard_value
              self.shard_value = shard
              super(local_id)
            ensure
              self.shard_value = old_shard_value
            end
          else
            shard.activate { super(local_id) }
          end
        else
          super
        end
      end

      def find_or_instantiator_by_attributes(match, attributes, *args)
        primary_shard.activate { super }
      end

      def exists?(conditions = :none)
        conditions = conditions.id if ::ActiveRecord::Base === conditions
        return false if !conditions

        if ::Rails.version >= '4.1'
          relation = apply_join_dependency(self, construct_join_dependency)
          return false if ::ActiveRecord::NullRelation === relation
        else
          join_dependency = construct_join_dependency_for_association_find
          relation = construct_relation_for_association_find(join_dependency)
        end

        relation = relation.except(:select, :order).select("1 AS one").limit(1)

        case conditions
        when Array, Hash
          relation = relation.where(conditions)
        else
          relation = relation.where(table[primary_key].eq(conditions)) if conditions != :none
        end

        args = [relation, "#{name} Exists"]
        args << relation.bind_values if ::Rails.version >= '4.1'
        activate { return true if connection.select_value(*args) }
        false
      rescue
        raise if ::Rails.version >= '4.1' || !(::ActiveRecord::ThrowResult === $!)
        false
      end
    end
  end
end
