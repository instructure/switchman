module Switchman
  module ActiveRecord
    module QueryMethods
      # shard_value is one of:
      #   A shard
      #   An array or relation of shards
      #   An AR object (query runs against that object's associated_shards)
      # shard_source_value is one of:
      #   :implicit - inferred from current shard when relation was created, or primary key where clause
      #   :explicit - explicit set on the relation
      #   :to_a - a special value that Relation#to_a uses when querying multiple shards to
      #           remove primary keys from conditions that aren't applicable to the current shard
      attr_accessor :shard_value, :shard_source_value

      def shard(value, source = :explicit)
        relation = clone
        relation.shard_value = value
        relation.shard_source_value = source
        if (primary_shard != relation.primary_shard || source == :to_a)
          relation.where_values = relation.transpose_predicates(relation.where_values, primary_shard, relation.primary_shard, source == :to_a) if !relation.where_values.empty?
          relation.having_values = relation.transpose_predicates(relation.where_values, primary_shard, relation.primary_shard, source == :to_a) if !relation.having_values.empty?
        end
        relation
      end

      # replace these with versions that call build_where on the
      # result relation, not the source relation (so build_where
      # is able to implicitly change the shard_value)
      def where(opts, *rest)
        return self if opts.blank?

        relation = clone
        relation.where_values += relation.build_where(opts, rest)
        relation
      end

      def having(opts, *rest)
        return self if opts.blank?

        relation = clone
        relation.having_values += relation.build_where(opts, rest)
        relation
      end

      def build_where(opts, other = [])
        source_shard = Shard.current(klass.shard_category)
        case opts
        when Hash, Arel::Nodes::Node
          predicates = super
          infer_shards_from_primary_key(predicates) if shard_source_value == :implicit && shard_value.is_a?(Shard)
          predicates = transpose_predicates(predicates, source_shard, primary_shard) if shard_source_value != :explicit
          predicates
        else
          super
        end
      end

      # the shard that where_values are relative to. if it's multiple shards, they're stored
      # relative to the first shard
      def primary_shard
        case shard_value
        when Shard, DefaultShard
          shard_value
        # associated_shards
        when ::ActiveRecord::Base
          shard_value.shard
        when Array
          shard_value.first
        when ::ActiveRecord::Relation
          Shard.default
        else
          raise ArgumentError("invalid shard value #{shard_value}")
        end
      end

      private
      def infer_shards_from_primary_key(predicates)
        primary_key = predicates.detect do |predicate|
          predicate.is_a?(Arel::Nodes::Binary) && predicate.left.is_a?(Arel::Attributes::Attribute) &&
          predicate.left.relation.engine == klass && klass.primary_key == predicate.left.name
        end
        if primary_key
          case primary_key.right
          when Array
            id_shards = Set.new
            primary_key.right.each do |value|
              local_id, id_shard = Shard.local_id_for(value)
              id_shard ||= Shard.current(klass.shard_category) if local_id
              id_shards << id_shard if id_shard
            end
            if id_shards.empty?
              return
            elsif id_shards.length == 1
              id_shard = id_shards.first
            # prefer to not change the shard
            elsif id_shards.include?(primary_shard)
              id_shards.delete(primary_shard)
              self.shard_value = [primary_shard] + id_shards.to_a
              return
            else
              id_shards = id_shards.to_a
              self.where_values = transpose_predicates(where_values, primary_shard, id_shards.first) if !where_values.empty?
              self.having_values = transpose_predicates(having_values, primary_shard, id_shards.first) if !having_values.empty?
              self.shard_value = id_shards
              return
            end
          else
            local_id, id_shard = Shard.local_id_for(primary_key.right)
            id_shard ||= Shard.current(klass.shard_category) if local_id
          end
          return if !id_shard || id_shard == primary_shard
          self.where_values = transpose_predicates(where_values, primary_shard, id_shard) if !where_values.empty?
          self.having_values = transpose_predicates(having_values, primary_shard, id_shard) if !having_values.empty?
          self.shard_value = id_shard
        end
      end

      # semi-private
      public
      def transpose_predicates(predicates, source_shard, target_shard, remove_nonlocal_primary_keys = false)
        predicates.map do |predicate|
          next predicate unless predicate.is_a?(Arel::Nodes::Binary) && predicate.left.is_a?(Arel::Attributes::Attribute)
          # primary key
          if predicate.left.relation.engine.primary_key == predicate.left.name
            remove = remove_nonlocal_primary_keys && predicate.left.relation.engine == klass
            new_right_value = case predicate.right
            when Array
              local_ids = []
              predicate.right.each do |value|
                local_id = Shard.relative_id_for(value, source_shard, target_shard)
                local_ids << local_id unless remove && local_id > Shard::IDS_PER_SHARD
              end
              local_ids
            else
              local_id = Shard.relative_id_for(predicate.right, source_shard, target_shard)
              local_id = [] if remove && local_id > Shard::IDS_PER_SHARD
              local_id
            end

            if new_right_value == predicate.right
              predicate
            else
              predicate.class.new(predicate.left, new_right_value)
            end
          else
            predicate
          end
        end
      end
    end
  end
end
