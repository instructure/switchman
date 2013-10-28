module Switchman
  module ActiveRecord
    module Calculations
      def self.included(klass)
        %w{execute_simple_calculation pluck}.each do |method|
          klass.alias_method_chain(method, :sharding)
        end
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

      # TODO: grouped calculations
      def execute_simple_calculation_with_sharding(operation, column_name, distinct)
        operation = operation.to_s.downcase
        if operation == "average"
          result = calculate_average(column_name, distinct)
        else
          result = self.activate{ |relation| relation.send(:execute_simple_calculation_without_sharding, operation, column_name, distinct) }
          if result.is_a?(Array)
            case operation
            when "count", "sum"
              result = result.sum
            when "minimum"
              result = result.min
            when "maximum"
              result = result.max
            end
          end
        end
        result
      end

      def calculate_average(column_name, distinct)
        # See activerecord#execute_simple_calculation
        relation = reorder(nil)
        column = aggregate_column(column_name)
        relation.select_values = [operation_over_aggregate_column(column, "average", distinct).as("average"),
                                  operation_over_aggregate_column(column, "count", distinct).as("count")]

        initial_results = relation.activate{ |rel| @klass.connection.select_all(rel) }
        if initial_results.is_a?(Array)
          initial_results.each do |r|
            r["average"] = type_cast_calculated_value(r["average"], nil, "average")
            r["count"] = type_cast_calculated_value(r["count"], nil, "count")
          end
          result = initial_results.map{|r| r["average"] * r["count"]}.sum / initial_results.map{|r| r["count"]}.sum
        else
          result = type_cast_calculated_value(initial_results["average"], nil, "average")
        end
        result
      end
    end
  end
end
