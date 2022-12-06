# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Relation
      def self.prepended(klass)
        klass::SINGLE_VALUE_METHODS.concat %i[shard shard_source]
      end

      def initialize(*, **)
        super
        self.shard_value = Shard.current(klass ? klass.connection_class_for_self : :primary) unless shard_value
        self.shard_source_value = :implicit unless shard_source_value
      end

      def clone
        result = super
        result.shard_value = Shard.current(klass ? klass.connection_class_for_self : :primary) unless shard_value
        result
      end

      def merge(*)
        relation = super
        if relation.shard_value != shard_value && relation.shard_source_value == :implicit
          relation.shard_value = shard_value
          relation.shard_source_value = shard_source_value
        end
        relation
      end

      def new(*, &block)
        primary_shard.activate(klass.connection_class_for_self) { super }
      end

      def create(*, &block)
        primary_shard.activate(klass.connection_class_for_self) { super }
      end

      def create!(*, &block)
        primary_shard.activate(klass.connection_class_for_self) { super }
      end

      def to_sql
        primary_shard.activate(klass.connection_class_for_self) { super }
      end

      def explain
        activate { |relation| relation.call_super(:explain, Relation) }
      end

      def load(&block)
        if !loaded? || (::Rails.version >= '7.0' && scheduled?)
          @records = activate { |relation| relation.send(:exec_queries, &block) }
          @loaded = true
        end

        self
      end

      %I[update_all delete_all].each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            result = self.activate(unordered: true) { |relation| relation.call_super(#{method.inspect}, Relation, *args) }
            result = result.sum if result.is_a?(Array)
            result
          end
        RUBY
      end

      def find_ids_in_ranges(options = {})
        is_integer = columns_hash[primary_key.to_s].type == :integer
        loose_mode = options[:loose] && is_integer
        # loose_mode: if we don't care about getting exactly batch_size ids in between
        # don't get the max - just get the min and add batch_size so we get that many _at most_
        values = loose_mode ? 'MIN(id)' : 'MIN(id), MAX(id)'

        batch_size = options[:batch_size].try(:to_i) || 1000
        quoted_primary_key = "#{klass.connection.quote_local_table_name(table_name)}.#{klass.connection.quote_column_name(primary_key)}"
        as_id = ' AS id' unless primary_key == 'id'
        subquery_scope = except(:select).select("#{quoted_primary_key}#{as_id}").reorder(primary_key.to_sym).limit(loose_mode ? 1 : batch_size)
        subquery_scope = subquery_scope.where("#{quoted_primary_key} <= ?", options[:end_at]) if options[:end_at]

        first_subquery_scope = if options[:start_at]
                                 subquery_scope.where("#{quoted_primary_key} >= ?",
                                                      options[:start_at])
                               else
                                 subquery_scope
                               end

        ids = connection.select_rows("SELECT #{values} FROM (#{first_subquery_scope.to_sql}) AS subquery").first

        while ids.first.present?
          ids.map!(&:to_i) if is_integer
          ids << (ids.first + batch_size) if loose_mode

          yield(*ids)
          last_value = ids.last
          next_subquery_scope = subquery_scope.where(["#{quoted_primary_key}>?", last_value])
          ids = connection.select_rows("SELECT #{values} FROM (#{next_subquery_scope.to_sql}) AS subquery").first
        end
      end

      def activate(unordered: false, &block)
        shards = all_shards
        if Array === shards && shards.length == 1
          if shards.first == DefaultShard || shards.first == Shard.current(klass.connection_class_for_self)
            yield(self, shards.first)
          else
            shards.first.activate(klass.connection_class_for_self) { yield(self, shards.first) }
          end
        else
          result_count = 0
          can_order = false
          result = Shard.with_each_shard(shards, [klass.connection_class_for_self]) do
            # don't even query other shards if we're already past the limit
            next if limit_value && result_count >= limit_value && order_values.empty?

            relation = shard(Shard.current(klass.connection_class_for_self), :to_a)
            relation.remove_nonlocal_primary_keys!
            # do a minimal query if possible
            relation = relation.limit(limit_value - result_count) if limit_value && !result_count.zero? && order_values.empty?

            shard_results = relation.activate(&block)

            if shard_results.present? && !unordered
              can_order ||= can_order_cross_shard_results? unless order_values.empty?
              raise OrderOnMultiShardQuery if !can_order && !order_values.empty? && result_count.positive?

              result_count += shard_results.is_a?(Array) ? shard_results.length : 1
            end
            shard_results
          end

          result = reorder_cross_shard_results(result) if can_order
          result.slice!(limit_value..-1) if limit_value
          result
        end
      end

      def can_order_cross_shard_results?
        # we only presume to be able to post-sort the most basic of orderings
        order_values.all? { |ov| ov.is_a?(::Arel::Nodes::Ordering) && ov.expr.is_a?(::Arel::Attributes::Attribute) }
      end

      def reorder_cross_shard_results(results)
        results.sort! do |l, r|
          result = 0
          order_values.each do |ov|
            if l.is_a?(::ActiveRecord::Base)
              a = l.attribute(ov.expr.name)
              b = r.attribute(ov.expr.name)
            else
              a, b = l, r
            end
            next if a == b

            if a.nil? || b.nil?
              result = 1 if a.nil?
              result *= -1 if ov.is_a?(::Arel::Nodes::Descending)
            else
              result = a <=> b
            end

            result *= -1 if ov.is_a?(::Arel::Nodes::Descending)
            break unless result.zero?
          end
          result
        end
      end
    end
  end
end
