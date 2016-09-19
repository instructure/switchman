module Switchman
  module ActiveRecord
    module FinderMethods
      def find_one(id, call_super: false)
        return super(id) unless klass.integral_id?
        return super(id) if call_super

        if shard_source_value != :implicit
          current_shard = Shard.current(klass.shard_category)
          result = self.activate do |relation, shard|
            current_id = Shard.relative_id_for(id, current_shard, shard)
            # skip the shard if the object can't be on it. unless we're only looking at one shard;
            # we might be expecting a shadow object
            next if current_id > Shard::IDS_PER_SHARD && self.all_shards.length > 1
            relation.send(:find_one, current_id, call_super: true)
          end
          if result.is_a?(Array)
            result = result.first
          end
          # we may have skipped all shards
          raise_record_not_found_exception!(id, 0, 1) unless result
          return result
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
          super(id)
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
        relation.activate { return true if connection.select_value(*args) }
        false
      rescue
        raise if ::Rails.version >= '4.1' || !(::ActiveRecord::ThrowResult === $!)
        false
      end
    end
  end
end
