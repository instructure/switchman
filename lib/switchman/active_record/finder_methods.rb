module Switchman
  module ActiveRecord
    module FinderMethods
      # find_one uses binds, so we can't depend on QueryMethods
      # catching it
      def find_one(id)
        local_id, shard = Shard.local_id_for(id)

        return super(local_id) if shard_source_value != :implicit

        if shard
          begin
            old_shard_value = shard_value
            self.shard_value = shard
            super(local_id)
          ensure
            self.shard_value = old_shard_value
          end
        else
          super
        end
      end

      def find_or_instantiator_by_attributes(match, attributes, *args)
        primary_shard.activate { super }
      end

      def exists?(id = false)
        id = id.id if ActiveRecord::Base === id
        return false if id.nil?

        join_dependency = construct_join_dependency_for_association_find
        relation = construct_relation_for_association_find(join_dependency)
        relation = relation.except(:select, :order).select("1 AS one").limit(1)

        case id
          when Array, Hash
            relation = relation.where(id)
          else
            relation = relation.where(table[primary_key].eq(id)) if id
        end

        activate { return true if connection.select_value(relation, "#{name} Exists") }
        false
      rescue ::ActiveRecord::ThrowResult
        false
      end
    end
  end
end
