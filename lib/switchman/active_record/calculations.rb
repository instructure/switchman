# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Calculations
      def pluck(*column_names)
        target_shard = Shard.current(klass.connection_class_for_self)
        shard_count = 0
        result = activate do |relation, shard|
          shard_count += 1
          results = relation.call_super(:pluck, Calculations, *column_names)
          if column_names.length > 1
            column_names.each_with_index do |column_name, idx|
              next unless klass.sharded_column?(column_name)

              results.each do |r|
                r[idx] = Shard.relative_id_for(r[idx], shard, target_shard)
              end
            end
          elsif klass.sharded_column?(column_names.first.to_s)
            results = results.map { |r| Shard.relative_id_for(r, shard, target_shard) }
          end
          results
        end
        result.uniq! if distinct_value && shard_count > 1
        result
      end

      def execute_simple_calculation(operation, column_name, distinct)
        operation = operation.to_s.downcase
        if operation == "average"
          result = calculate_simple_average(column_name, distinct)
        else
          result = activate do |relation|
            relation.call_super(:execute_simple_calculation, Calculations, operation, column_name, distinct)
          end
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

      def calculate_simple_average(column_name, distinct)
        # See activerecord#execute_simple_calculation
        relation = except(:order)
        column = aggregate_column(column_name)
        relation.select_values = [operation_over_aggregate_column(column, "average", distinct).as("average"),
                                  operation_over_aggregate_column(column, "count", distinct).as("count")]

        initial_results = relation.activate { |rel| klass.connection.select_all(rel) }
        if initial_results.is_a?(Array)
          initial_results.each do |r|
            r["average"] = type_cast_calculated_value_switchman(r["average"], column_name, "average")
            r["count"] = type_cast_calculated_value_switchman(r["count"], column_name, "count")
          end
          result = initial_results.sum { |r| r["average"] * r["count"] } / initial_results.sum do |r|
                                                                             r["count"]
                                                                           end
        else
          result = type_cast_calculated_value_switchman(initial_results.first["average"], column_name, "average")
        end
        result
      end

      # See activerecord#execute_grouped_calculation
      def execute_grouped_calculation(operation, column_name, distinct)
        opts = grouped_calculation_options(operation.to_s.downcase, column_name, distinct)

        relation = build_grouped_calculation_relation(opts)
        target_shard = Shard.current

        rows = relation.activate do |rel, shard|
          calculated_data = klass.connection.select_all(rel)

          if opts[:association]
            key_ids     = calculated_data.collect { |row| row[opts[:group_aliases].first] }
            key_records = opts[:association].klass.base_class.where(id: key_ids)
            key_records = key_records.to_h { |r| [Shard.relative_id_for(r, shard, target_shard), r] }
          end

          calculated_data.map do |row|
            row[opts[:aggregate_alias]] = type_cast_calculated_value_switchman(
              row[opts[:aggregate_alias]], column_name, opts[:operation]
            )
            row["count"] = row["count"].to_i if opts[:operation] == "average"

            opts[:group_columns].each do |aliaz, _type, group_column_name|
              if opts[:associated] && (aliaz == opts[:group_aliases].first)
                row[aliaz] = key_records[Shard.relative_id_for(row[aliaz], shard, target_shard)]
              elsif group_column_name && klass.sharded_column?(group_column_name)
                row[aliaz] = Shard.relative_id_for(row[aliaz], shard, target_shard)
              end
            end
            row
          end
        end

        compact_grouped_calculation_rows(rows, opts)
      end

      def ids
        return super unless klass.sharded_primary_key?

        if loaded?
          result = records.map do |record|
            Shard.relative_id_for(record._read_attribute(primary_key),
                                  record.shard,
                                  Shard.current(klass.connection_class_for_self))
          end
          return @async ? Promise::Complete.new(result) : result
        end

        if has_include?(primary_key)
          relation = apply_join_dependency.group(primary_key)
          return relation.ids
        end

        columns = arel_columns([primary_key])
        base_shard = Shard.current(klass.connection_class_for_self)
        activate do |r|
          relation = r.spawn
          relation.select_values = columns

          result = if relation.where_clause.contradiction?
                     ::ActiveRecord::Result.empty
                   else
                     skip_query_cache_if_necessary do
                       klass.connection.select_all(relation, "#{klass.name} Ids", async: @async)
                     end
                   end

          result.then do |res|
            type_cast_pluck_values(res, columns).map { |id| Shard.relative_id_for(id, Shard.current, base_shard) }
          end
        end
      end

      private

      def type_cast_calculated_value_switchman(value, column_name, operation)
        column = aggregate_column(column_name)
        type ||= column.try(:type_caster) ||
                 lookup_cast_type_from_join_dependencies(column_name.to_s) || ::ActiveRecord::Type.default_value
        type_cast_calculated_value(value, operation, type)
      end

      def column_name_for(field)
        field.respond_to?(:name) ? field.name.to_s : field.to_s.split(".").last
      end

      def grouped_calculation_options(operation, column_name, distinct)
        opts = { operation:, column_name:, distinct: }

        column_alias_tracker = ::ActiveRecord::Calculations::ColumnAliasTracker.new(connection)

        opts[:aggregate_alias] = aggregate_alias_for(operation, column_name, column_alias_tracker)
        group_attrs = group_values
        if group_attrs.first.respond_to?(:to_sym)
          association  = klass.reflect_on_association(group_attrs.first.to_sym)
          # only count belongs_to associations
          associated   = group_attrs.size == 1 && association && association.macro == :belongs_to
          group_fields = Array(associated ? association.foreign_key : group_attrs)
        else
          group_fields = group_attrs
        end

        group_aliases = group_fields.map do |field|
          field = connection.visitor.compile(field) if ::Arel.arel_node?(field)
          column_alias_tracker.alias_for(field.to_s.downcase)
        end
        group_columns = group_aliases.zip(group_fields).map do |aliaz, field|
          [aliaz, type_for(field), column_name_for(field)]
        end
        opts.merge!(association:,
                    associated:,
                    group_aliases:,
                    group_columns:,
                    group_fields:)

        opts
      end

      def aggregate_alias_for(operation, column_name, column_alias_tracker)
        if operation == "count" && column_name == :all
          "count_all"
        elsif operation == "average"
          "average"
        else
          column_alias_tracker.alias_for("#{operation} #{column_name}")
        end
      end

      def build_grouped_calculation_relation(opts)
        group = opts[:group_fields]

        select_values = [
          operation_over_aggregate_column(
            aggregate_column(opts[:column_name]),
            opts[:operation],
            opts[:distinct]
          ).as(opts[:aggregate_alias])
        ]
        if opts[:operation] == "average"
          # include count in average so we can recalculate the average
          # across all shards if needed
          select_values << operation_over_aggregate_column(
            aggregate_column(opts[:column_name]),
            "count",
            opts[:distinct]
          ).as("count")
        end

        haves = having_clause.send(:predicates)
        select_values += select_values unless haves.empty?
        select_values.concat(opts[:group_fields].zip(opts[:group_aliases]).map do |field, aliaz|
          if field.respond_to?(:as)
            field.as(aliaz)
          else
            "#{field} AS #{aliaz}"
          end
        end)

        relation = except(:group)
        relation.group_values = group
        relation.select_values = select_values
        relation
      end

      def compact_grouped_calculation_rows(rows, opts)
        result = ::ActiveSupport::OrderedHash.new
        rows.each do |row|
          key = opts[:group_columns].map { |aliaz, _column| row[aliaz] }
          key = key.first if key.size == 1
          value = row[opts[:aggregate_alias]]

          if opts[:operation] == "average"
            if result.key?(key)
              old_value, old_count = result[key]
              new_count = old_count + row["count"]
              new_value = ((old_value * old_count) + (value * row["count"])) / new_count
              result[key] = [new_value, new_count]
            else
              result[key] = [value, row["count"]]
            end
          elsif result.key?(key)
            case opts[:operation]
            when "count", "sum"
              result[key] += value
            when "minimum"
              result[key] = value if value < result[key]
            when "maximum"
              result[key] = value if value > result[key]
            end
          else
            result[key] = value
          end
        end

        result.transform_values!(&:first) if opts[:operation] == "average"

        result
      end
    end
  end
end
