# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module FinderMethods
      def find_one(id)
        return super(id) unless klass.integral_id?

        if shard_source_value != :implicit
          current_shard = Shard.current(klass.connection_classes)
          result = self.activate do |relation, shard|
            current_id = Shard.relative_id_for(id, current_shard, shard)
            # current_id will be nil for non-integral id
            next unless current_id
            # skip the shard if the object can't be on it. unless we're only looking at one shard;
            # we might be expecting a shadow object
            next if current_id > Shard::IDS_PER_SHARD && self.all_shards.length > 1
            relation.call_super(:find_one, FinderMethods, current_id)
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
          shard.activate { super(local_id) }
        else
          super(id)
        end
      end

      def find_some_ordered(ids)
        current_shard = Shard.current(klass.connection_classes)
        ids = ids.map{|id| Shard.relative_id_for(id, current_shard, current_shard)}
        super(ids)
      end

      def find_or_instantiator_by_attributes(match, attributes, *args)
        primary_shard.activate { super }
      end

      def exists?(conditions = :none)
        conditions = conditions.id if ::ActiveRecord::Base === conditions
        return false if !conditions

        relation = apply_join_dependency(eager_loading: false)
        return false if ::ActiveRecord::NullRelation === relation

        relation = relation.except(:select, :order).select("1 AS one").limit(1)

        case conditions
        when Array, Hash
          relation = relation.where(conditions)
        else
          relation = relation.where(table[primary_key].eq(conditions)) if conditions != :none
        end

        relation.activate do |shard_rel|
          return true if connection.select_value(shard_rel.arel, "#{name} Exists")
        end
        false
      end
    end
  end
end
