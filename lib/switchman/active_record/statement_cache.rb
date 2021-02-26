# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module StatementCache
      module ClassMethods
        def create(connection, &block)
          relation = block.call ::ActiveRecord::StatementCache::Params.new

          _query_builder, binds = connection.cacheable_query(self, relation.arel)
          bind_map = ::ActiveRecord::StatementCache::BindMap.new(binds)
          new(relation.arel, bind_map, relation.klass)
        end
      end

      def initialize(arel, bind_map, klass = nil)
        @arel = arel
        @bind_map = bind_map
        @klass = klass
        @qualified_query_builders = {}
      end

      # since the StatememtCache is only implemented
      # for basic relations in AR::Base#find, AR::Base#find_by and AR::Association#get_records,
      # we can make some assumptions about the shard source
      # (e.g. infer from the primary key or use the current shard)

      def execute(*args)
        params, connection = args
        klass = @klass
        target_shard = nil
        if (primary_index = bind_map.primary_value_index)
          primary_value = params[primary_index]
          target_shard = Shard.local_id_for(primary_value)[1]
        end
        current_shard = Shard.current(klass.connection_classes)
        target_shard ||= current_shard

        bind_values = bind_map.bind(params, current_shard, target_shard)

        target_shard.activate(klass.connection_classes) do
          sql = qualified_query_builder(target_shard, klass).sql_for(bind_values, connection)
          klass.find_by_sql(sql, bind_values)
        end
      end

      def qualified_query_builder(shard, klass)
        @qualified_query_builders[shard.id] ||= klass.connection.cacheable_query(self.class, @arel).first
      end

      module BindMap
        # performs id transposition here instead of query_methods.rb
        def bind(values, current_shard, target_shard)
          bas = @bound_attributes.dup
          @indexes.each_with_index do |offset, i|
            ba = bas[offset]
            new_value = if ba.is_a?(::ActiveRecord::Relation::QueryAttribute) && ba.value.sharded
                          Shard.relative_id_for(values[i], current_shard, target_shard || current_shard)
                        else
                          values[i]
                        end
            bas[offset] = ba.with_cast_value(new_value)
          end
          bas
        end

        def primary_value_index
          primary_ba_index = @bound_attributes.index do |ba|
            ba.is_a?(::ActiveRecord::Relation::QueryAttribute) && ba.value.primary
          end
          @indexes.index(primary_ba_index) if primary_ba_index
        end
      end
    end
  end
end
