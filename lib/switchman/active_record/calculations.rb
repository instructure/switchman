module Switchman
  module ActiveRecord
    module Calculations

      def pluck(*column_names)
        target_shard = Shard.current(klass.shard_category)
        shard_count = 0
        result = self.activate do |relation, shard|
          shard_count += 1
          results = relation.call_super(:pluck, Calculations, *column_names)
          if column_names.length > 1
            column_names.each_with_index do |column_name, idx|
              if klass.sharded_column?(column_name)
                results.each{|result| result[idx] = Shard.relative_id_for(result[idx], shard, target_shard)}
              end
            end
          else
            if klass.sharded_column?(column_names.first.to_s)
              results = results.map{|result| Shard.relative_id_for(result, shard, target_shard)}
            end
          end
          results
        end
        if distinct_value && shard_count > 1
          result.uniq!
        end
        result
      end

      def execute_simple_calculation(operation, column_name, distinct)
        operation = operation.to_s.downcase
        if operation == "average"
          result = calculate_simple_average(column_name, distinct)
        else
          result = self.activate{ |relation| relation.call_super(:execute_simple_calculation, Calculations, operation, column_name, distinct) }
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
        relation = reorder(nil)
        column = aggregate_column(column_name)
        relation.select_values = [operation_over_aggregate_column(column, "average", distinct).as("average"),
                                  operation_over_aggregate_column(column, "count", distinct).as("count")]

        initial_results = relation.activate{ |rel| klass.connection.select_all(rel) }
        if initial_results.is_a?(Array)
          initial_results.each do |r|
            r["average"] = type_cast_calculated_value(r["average"], type_for(column_name), "average")
            r["count"] = type_cast_calculated_value(r["count"], type_for(column_name), "count")
          end
          result = initial_results.map{|r| r["average"] * r["count"]}.sum / initial_results.map{|r| r["count"]}.sum
        else
          result = type_cast_calculated_value(initial_results.first["average"], type_for(column_name), "average")
        end
        result
      end

      # See activerecord#execute_grouped_calculation
      def execute_grouped_calculation(operation, column_name, distinct)
        opts = grouped_calculation_options(operation.to_s.downcase, column_name, distinct)

        relation = build_grouped_calculation_relation(opts)
        target_shard = Shard.current(:primary)

        rows = relation.activate do |rel, shard|
          calculated_data = klass.connection.select_all(rel)

          if opts[:association]
            key_ids     = calculated_data.collect { |row| row[opts[:group_aliases].first] }
            key_records = opts[:association].klass.base_class.where(:id => key_ids)
            key_records = Hash[key_records.map { |r| [Shard.relative_id_for(r, shard, target_shard), r] }]
          end

          calculated_data.map do |row|
            row[opts[:aggregate_alias]] = type_cast_calculated_value(
                row[opts[:aggregate_alias]], type_for(opts[:column_name]), opts[:operation])
            row['count'] = row['count'].to_i if opts[:operation] == 'average'

            opts[:group_columns].each do |aliaz, type, column_name|
              if opts[:associated] && (aliaz == opts[:group_aliases].first)
                row[aliaz] = key_records[Shard.relative_id_for(row[aliaz], shard, target_shard)]
              elsif column_name && @klass.sharded_column?(column_name)
                row[aliaz] = Shard.relative_id_for(type_cast_calculated_value(row[aliaz], type), shard, target_shard)
              end
            end
            row
          end
        end

        compact_grouped_calculation_rows(rows, opts)
      end

      private

      def column_name_for(field)
        field.respond_to?(:name) ? field.name.to_s : field.to_s.split('.').last
      end

      def grouped_calculation_options(operation, column_name, distinct)
        opts = {:operation => operation, :column_name => column_name, :distinct => distinct}

        opts[:aggregate_alias] = aggregate_alias_for(operation, column_name)
        group_attrs = group_values
        if group_attrs.first.respond_to?(:to_sym)
          association  = klass.reflect_on_association(group_attrs.first.to_sym)
          associated   = group_attrs.size == 1 && association && association.macro == :belongs_to # only count belongs_to associations
          group_fields = Array(associated ? association.foreign_key : group_attrs)
        else
          group_fields = group_attrs
        end

        # to_s is because Rails 5 returns a string but Rails 6 returns a symbol.
        group_aliases = group_fields.map { |field| column_alias_for(field.downcase.to_s).to_s }
        group_columns = group_aliases.zip(group_fields).map { |aliaz, field|
          [aliaz, type_for(field), column_name_for(field)]
        }
        opts.merge!(:association => association, :associated => associated,
         :group_aliases => group_aliases, :group_columns => group_columns,
         :group_fields => group_fields)

        opts
      end

      def aggregate_alias_for(operation, column_name)
        if operation == 'count' && column_name == :all
          'count_all'
        elsif operation == 'average'
          'average'
        else
          column_alias_for("#{operation} #{column_name}")
        end
      end

      def build_grouped_calculation_relation(opts)
        group = opts[:group_fields]

        select_values = [
            operation_over_aggregate_column(
                aggregate_column(opts[:column_name]),
                opts[:operation],
                opts[:distinct]).as(opts[:aggregate_alias])
        ]
        if opts[:operation ]== 'average'
          # include count in average so we can recalculate the average
          # across all shards if needed
          select_values << operation_over_aggregate_column(
              aggregate_column(opts[:column_name]),
              'count', opts[:distinct]).as('count')
        end

        haves = having_clause.send(:predicates)
        select_values += select_values unless haves.empty?
        select_values.concat opts[:group_fields].zip(opts[:group_aliases]).map { |field,aliaz|
          if field.respond_to?(:as)
            field.as(aliaz)
          else
            "#{field} AS #{aliaz}"
          end
        }

        relation = except(:group)
        relation.group_values = group
        relation.select_values = select_values
        relation
      end

      def compact_grouped_calculation_rows(rows, opts)
        result = ::ActiveSupport::OrderedHash.new
        rows.each do |row|
          key = opts[:group_columns].map { |aliaz, column| row[aliaz] }
          key = key.first if key.size == 1
          value = row[opts[:aggregate_alias]]

          if opts[:operation] == 'average'
            if result.has_key?(key)
              old_value, old_count = result[key]
              new_count = old_count + row['count']
              new_value = ((old_value * old_count) + (value * row['count'])) / new_count
              result[key] = [new_value, new_count]
            else
              result[key] = [value, row['count']]
            end
          else
            if result.has_key?(key)
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
        end

        if opts[:operation] == 'average'
          result = Hash[result.map{|k, v| [k, v.first]}]
        end

        result
      end


    end
  end
end
