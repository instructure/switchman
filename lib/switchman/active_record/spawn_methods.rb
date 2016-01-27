module Switchman
  module ActiveRecord
    module SpawnMethods
      def shard_values_for_merge(r)
        if shard_value != r.shard_value
          if r.shard_source_value == :implicit
            final_shard_value = shard_value
            final_primary_shard = primary_shard
            final_shard_source_value = shard_source_value
          elsif shard_source_value == :implicit
            final_shard_value = r.shard_value
            final_primary_shard = r.primary_shard
            final_shard_source_value = r.shard_source_value
          else
            final_shard_source_value = [:explicit, :association].detect do |source_value|
              shard_source_value == source_value || r.shard_source_value == source_value
            end
            raise "unknown shard_source_value" unless final_shard_source_value

            # have to merge shard_value
            lhs_shard_value = all_shards
            rhs_shard_value = r.all_shards
            if (::ActiveRecord::Relation === lhs_shard_value &&
                ::ActiveRecord::Relation === rhs_shard_value)
              final_shard_value = lhs_shard_value.merge(rhs_shard_value)
              final_primary_shard = Shard.default
            else
              final_shard_value = lhs_shard_value.to_a & rhs_shard_value.to_a
              final_primary_shard = final_shard_value.first
              final_shard_value = final_shard_value.first if final_shard_value.length == 1
            end
          end
        elsif shard_source_value != r.shard_source_value
          final_shard_source_value = [:explicit, :association, :implicit].detect do |source_value|
            shard_source_value == source_value || r.shard_source_value == source_value
          end
          raise "unknown shard_source_value" unless final_shard_source_value
        else
          # nothing fancy
        end

        [final_shard_value, final_primary_shard, final_shard_source_value]
      end

      def merge!(r)
        return super unless ::ActiveRecord::Relation === r

        # have to figure out shard stuff *before* conditions are merged
        final_shard_value, final_primary_shard, final_shard_source_value = shard_values_for_merge(r)

        return super unless final_shard_source_value

        if !final_shard_value
          super
          self.shard_source_value = final_shard_source_value
          return self
        end

        return none! if final_shard_value == []

        # change the primary shard if necessary before merging
        if primary_shard != final_primary_shard && r.primary_shard != final_primary_shard
          shard!(final_primary_shard)
          r = r.shard(final_primary_shard)
          super(r)
        elsif primary_shard != final_primary_shard
          shard!(final_primary_shard)
          super(r)
        elsif r.primary_shard != final_primary_shard
          r = r.shard(final_primary_shard)
          super(r)
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
