# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module SpawnMethods
      def shard_values_for_merge(rhs)
        if shard_value != rhs.shard_value
          if rhs.shard_source_value == :implicit
            final_shard_value = shard_value
            final_primary_shard = primary_shard
            final_shard_source_value = shard_source_value
          elsif shard_source_value == :implicit
            final_shard_value = rhs.shard_value
            final_primary_shard = rhs.primary_shard
            final_shard_source_value = rhs.shard_source_value
          else
            final_shard_source_value = %i[explicit association].detect do |source_value|
              shard_source_value == source_value || rhs.shard_source_value == source_value
            end
            raise "unknown shard_source_value" unless final_shard_source_value

            # have to merge shard_value
            lhs_shard_value = all_shards
            rhs_shard_value = rhs.all_shards
            if ::ActiveRecord::Relation === lhs_shard_value &&
               ::ActiveRecord::Relation === rhs_shard_value
              final_shard_value = lhs_shard_value.merge(rhs_shard_value)
              final_primary_shard = Shard.default
            else
              final_shard_value = lhs_shard_value.to_a & rhs_shard_value.to_a
              final_primary_shard = final_shard_value.first
              final_shard_value = final_shard_value.first if final_shard_value.length == 1
            end
          end
        elsif shard_source_value != rhs.shard_source_value
          final_shard_source_value = %i[explicit association implicit].detect do |source_value|
            shard_source_value == source_value || rhs.shard_source_value == source_value
          end
          raise "unknown shard_source_value" unless final_shard_source_value
        end

        [final_shard_value, final_primary_shard, final_shard_source_value]
      end

      def merge!(rhs)
        return super unless ::ActiveRecord::Relation === rhs

        # have to figure out shard stuff *before* conditions are merged
        final_shard_value, final_primary_shard, final_shard_source_value = shard_values_for_merge(rhs)

        return super unless final_shard_source_value

        unless final_shard_value
          super
          self.shard_source_value = final_shard_source_value
          return self
        end

        return none! if final_shard_value == []

        # change the primary shard if necessary before merging
        if primary_shard != final_primary_shard && rhs.primary_shard != final_primary_shard
          shard!(final_primary_shard)
          rhs = rhs.shard(final_primary_shard)
          super(rhs)
        elsif primary_shard != final_primary_shard
          shard!(final_primary_shard)
          super(rhs)
        elsif rhs.primary_shard != final_primary_shard
          rhs = rhs.shard(final_primary_shard)
          super(rhs)
        else
          super
        end

        self.shard_value = final_shard_value
        self.shard_source_value = final_shard_source_value

        self
      end
    end
  end
end
