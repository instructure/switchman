module Switchman
  module ActiveRecord
    module Relation
      def self.included(klass)
        klass::SINGLE_VALUE_METHODS.concat [ :shard, :shard_source ]
        %w{initialize exec_queries update_all delete_all}.each do |method|
          klass.alias_method_chain(method, :sharding)
        end
      end

      def initialize_with_sharding(klass, table)
        initialize_without_sharding(klass, table)
        self.shard_value = Shard.current(klass.shard_category)
        self.shard_source_value = :implicit
      end

      def merge(*args)
        relation = super
        if relation.shard_value != self.shard_value && relation.shard_source_value == :implicit
          relation.shard_value = self.shard_value
          relation.shard_source_value = self.shard_source_value
        end
        relation
      end

      def exec_queries_with_sharding
        return @records if loaded?
        results = self.activate{|relation| relation.send(:exec_queries_without_sharding) }
        case shard_value
        when Array, ::ActiveRecord::Relation, ::ActiveRecord::Base
          @records = results
          @loaded = true
        end
        results
      end

      %w{update_all delete_all}.each do |method|
        class_eval <<-RUBY
          def #{method}_with_sharding(*args)
            self.activate{|relation| relation.#{method}_without_sharding(*args)}
          end
        RUBY
      end

      def activate(&block)
        case shard_value
        when DefaultShard, Shard.current(klass.shard_category)
          yield(self, shard_value)
        when Shard
          shard_value.activate(klass.shard_category) { yield(self, shard_value) }
        when Array, ::ActiveRecord::Relation, ::ActiveRecord::Base
          # TODO: implement local limit to avoid querying extra shards
          if shard_value.is_a?(::ActiveRecord::Base)
            if shard_value.respond_to?(:associated_shards)
              shards = shard_value.associated_shards
            else
              shards = [shard_value.shard]
            end
          else
            shards = shard_value
          end
          Shard.with_each_shard(shards, [klass.shard_category]) do
            shard(Shard.current(klass.shard_category), :to_a).activate(&block)
          end
        end
      end
    end
  end
end
