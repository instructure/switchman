module Switchman
  module ActiveRecord
    module StatementCache
      # since the StatememtCache is only implemented
      # for basic relations in AR::Base#find, AR::Base#find_by and AR::Association#get_records,
      # we can make some assumptions about the shard source
      # (e.g. infer from the primary key or use the current shard)

      def execute(params, klass, connection)
        target_shard = nil
        if primary_index = bind_map.primary_value_index
          primary_value = params[primary_index]
          target_shard = Shard.local_id_for(primary_value)[1]
        end
        current_shard = Shard.current(klass.shard_category)
        target_shard ||= current_shard

        bind_values = bind_map.bind(params, current_shard, target_shard)

        sql = query_builder.sql_for(bind_values, connection)
        target_shard.activate(klass.shard_category) do
          klass.find_by_sql(sql, bind_values)
        end
      end

      module BindMap
        # performs id transposition here instead of query_methods.rb
        def bind(values, current_shard, target_shard)
          if ::Rails.version >= '5'
            bas = @bound_attributes.dup
            @indexes.each_with_index do |offset,i|
              ba = bas[offset]
              if ba.is_a?(::ActiveRecord::Relation::QueryAttribute) && ba.value.sharded
                new_value = Shard.relative_id_for(values[i], current_shard, target_shard || current_shard)
              else
                new_value = values[i]
              end
              bas[offset] = ba.with_cast_value(new_value)
            end
            bas
          else
            bvs = @bind_values.map { |pair| pair.dup }
            @indexes.each_with_index do |offset,i|
              bv = bvs[offset]
              if bv[1].sharded
                bv[1] = Shard.relative_id_for(values[i], current_shard, target_shard || current_shard)
              else
                bv[1] = values[i]
              end
            end
            bvs
          end
        end

        def primary_value_index
          if ::Rails.version >= '5'
            primary_ba_index = @bound_attributes.index do |ba|
              ba.is_a?(::ActiveRecord::Relation::QueryAttribute) && ba.value.primary
            end
            if primary_ba_index
              @indexes.index(primary_ba_index)
            end
          else
            if primary_bv_index = @bind_values.index{|col, sub| sub.primary}
              @indexes.index(primary_bv_index)
            end
          end
        end
      end
    end
  end
end
