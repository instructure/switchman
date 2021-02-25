# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Relation
      def self.prepended(klass)
        klass::SINGLE_VALUE_METHODS.concat [ :shard, :shard_source ]
      end

      def initialize(*, **)
        super
        self.shard_value = Shard.current(klass ? klass.connection_classes : :primary) unless shard_value
        self.shard_source_value = :implicit unless shard_source_value
      end

      def clone
        result = super
        result.shard_value = Shard.current(klass ? klass.connection_classes : :primary) unless shard_value
        result
      end

      def merge(*)
        relation = super
        if relation.shard_value != self.shard_value && relation.shard_source_value == :implicit
          relation.shard_value = self.shard_value
          relation.shard_source_value = self.shard_source_value
        end
        relation
      end

      def new(*, &block)
        primary_shard.activate(klass.connection_classes) { super }
      end

      def create(*, &block)
        primary_shard.activate(klass.connection_classes) { super }
      end

      def create!(*, &block)
        primary_shard.activate(klass.connection_classes) { super }
      end

      def to_sql
        primary_shard.activate(klass.connection_classes) { super }
      end

      def explain
        self.activate { |relation| relation.call_super(:explain, Relation) }
      end

      def records
        return @records if loaded?
        results = self.activate { |relation| relation.call_super(:records, Relation) }
        case shard_value
        when Array, ::ActiveRecord::Relation, ::ActiveRecord::Base
          @records = results
          @loaded = true
        end
        results
      end

      %I{update_all delete_all}.each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            result = self.activate { |relation| relation.call_super(#{method.inspect}, Relation, *args) }
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
        values = loose_mode ? "MIN(id)" : "MIN(id), MAX(id)"

        batch_size = options[:batch_size].try(:to_i) || 1000
        quoted_primary_key = "#{klass.connection.quote_local_table_name(table_name)}.#{klass.connection.quote_column_name(primary_key)}"
        as_id = " AS id" unless primary_key == 'id'
        subquery_scope = except(:select).select("#{quoted_primary_key}#{as_id}").reorder(primary_key.to_sym).limit(loose_mode ? 1 : batch_size)
        subquery_scope = subquery_scope.where("#{quoted_primary_key} <= ?", options[:end_at]) if options[:end_at]

        first_subquery_scope = options[:start_at] ? subquery_scope.where("#{quoted_primary_key} >= ?", options[:start_at]) : subquery_scope

        ids = connection.select_rows("SELECT #{values} FROM (#{first_subquery_scope.to_sql}) AS subquery").first

        while ids.first.present?
          ids.map!(&:to_i) if is_integer
          ids << ids.first + batch_size if loose_mode

          yield(*ids)
          last_value = ids.last
          next_subquery_scope = subquery_scope.where(["#{quoted_primary_key}>?", last_value])
          ids = connection.select_rows("SELECT #{values} FROM (#{next_subquery_scope.to_sql}) AS subquery").first
        end
      end

      def activate(&block)
        shards = all_shards
        if (Array === shards && shards.length == 1)
          if shards.first == DefaultShard || shards.first == Shard.current(klass.connection_classes)
            yield(self, shards.first)
          else
            shards.first.activate(klass.connection_classes) { yield(self, shards.first) }
          end
        else
          # TODO: implement local limit to avoid querying extra shards
          Shard.with_each_shard(shards, [klass.connection_classes]) do
            shard(Shard.current(klass.connection_classes), :to_a).activate(&block)
          end
        end
      end
    end
  end
end
