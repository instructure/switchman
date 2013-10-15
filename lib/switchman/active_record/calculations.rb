module Switchman
  module ActiveRecord
    module Calculations
      def self.included(klass)
        klass.alias_method_chain :pluck, :sharding
      end

      def pluck_with_sharding(column_name)
        target_shard = Shard.current(klass.shard_category)
        self.activate do |relation, shard|
          results = relation.pluck_without_sharding(column_name)
          if klass.sharded_column?(column_name.to_s)
            results = results.map{|result| Shard.relative_id_for(result, shard, target_shard)}
          end
          results
        end
      end
    end
  end
end
