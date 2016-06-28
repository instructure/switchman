module Switchman
  module ActiveRecord
    module WhereClauseFactory
      attr_writer :scope

      def build(opts, other = [])
        case opts
        when String, Array
          values = Hash === other.first ? other.first.values : other

          values.grep(ActiveRecord::Relation) do |rel|
            # serialize subqueries against the same shard as the outer query is currently
            # targeted to run against
            if rel.shard_source_value == :implicit && rel.primary_shard != @scope.primary_shard
              rel.shard!(@scope.primary_shard)
            end
          end

          super
        when Hash, ::Arel::Nodes::Node
          where_clause = super
          predicates = where_clause.send(:predicates)
          @scope.send(:infer_shards_from_primary_key, predicates, where_clause.binds) if @scope.shard_source_value == :implicit && @scope.shard_value.is_a?(Shard)
          predicates = @scope.transpose_predicates(predicates, nil, @scope.primary_shard, false, where_clause.binds) if @scope.shard_source_value != :explicit
          where_clause.instance_variable_set(:@predicates, predicates)
          where_clause
        else
          super
        end
      end
    end
  end
end
