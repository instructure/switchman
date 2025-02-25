# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module FinderMethods
      def find_one(id)
        return super unless klass.integral_id?

        if shard_source_value != :implicit
          current_shard = Shard.current(klass.connection_class_for_self)
          result = activate do |relation, shard|
            current_id = Shard.relative_id_for(id, current_shard, shard)
            # current_id will be nil for non-integral id
            next unless current_id
            # skip the shard if the object can't be on it. unless we're only looking at one shard;
            # we might be expecting a shadow object
            next if current_id > Shard::IDS_PER_SHARD && all_shards.length > 1

            relation.call_super(:find_one, FinderMethods, current_id)
          end
          result = result.first if result.is_a?(Array)
          # we may have skipped all shards
          raise_record_not_found_exception!(id, 0, 1) unless result
          return result
        end

        local_id, shard = Shard.local_id_for(id)
        if shard
          shard.activate { super(local_id) }
        else
          super
        end
      end

      def find_some_ordered(ids)
        current_shard = Shard.current(klass.connection_class_for_self)
        ids = ids.map { |id| Shard.relative_id_for(id, current_shard, current_shard) }
        super
      end

      def find_or_instantiator_by_attributes(match, attributes, *args)
        primary_shard.activate { super }
      end

      if ::Rails.version < "7.1"
        def exists?(conditions = :none)
          conditions = conditions.id if ::ActiveRecord::Base === conditions
          return false unless conditions

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
      else
        def exists?(conditions = :none)
          return false if @none

          if Base === conditions
            raise ArgumentError, <<-TEXT.squish
              You are passing an instance of ActiveRecord::Base to `exists?`.
              Please pass the id of the object by calling `.id`.
            TEXT
          end

          return false if !conditions || limit_value == 0 # rubocop:disable Style/NumericPredicate

          if eager_loading?
            relation = apply_join_dependency(eager_loading: false)
            return relation.exists?(conditions)
          end

          relation = construct_relation_for_exists(conditions)
          return false if relation.where_clause.contradiction?

          relation.activate do |shard_rel|
            return true if skip_query_cache_if_necessary do
                             connection.select_rows(shard_rel.arel, "#{name} Exists?").size == 1
                           end
          end
          false
        end
      end
    end
  end
end
