# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module QueryMethods
      # shard_value is one of:
      #   A shard
      #   An array or relation of shards
      #   An AR object (query runs against that object's associated_shards)
      # shard_source_value is one of:
      #   :implicit    - inferred from current shard when relation was created, or primary key where clause
      #   :explicit    - explicit set on the relation
      #   :association - a special value that scopes from associations use to use slightly different logic
      #                  for foreign key transposition
      #   :to_a        - a special value that Relation#to_a uses when querying multiple shards to
      #                  remove primary keys from conditions that aren't applicable to the current shard
      def shard_value
        @values[:shard]
      end

      def shard_source_value
        @values[:shard_source]
      end

      def shard_value=(value)
        raise ::ActiveRecord::ImmutableRelation if @loaded

        @values[:shard] = value
      end

      def shard_source_value=(value)
        raise ::ActiveRecord::ImmutableRelation if @loaded

        @values[:shard_source] = value
      end

      def shard(value, source = :explicit)
        spawn.shard!(value, source)
      end

      def shard!(value, source = :explicit)
        raise ArgumentError, "shard can't be nil" unless value

        old_primary_shard = primary_shard
        self.shard_value = value
        self.shard_source_value = source
        transpose_predicates(nil, old_primary_shard, primary_shard, remove_nonlocal_primary_keys: source == :to_a) if old_primary_shard != primary_shard || source == :to_a
        self
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
        when nil
          Shard.current(klass.connection_class_for_self)
        else
          raise ArgumentError, "invalid shard value #{shard_value}"
        end
      end

      # the shard value as an array or a relation
      def all_shards
        case shard_value
        when Shard, DefaultShard
          [shard_value]
        when ::ActiveRecord::Base
          shard_value.respond_to?(:associated_shards) ? shard_value.associated_shards : [shard_value.shard]
        when nil
          [Shard.current(klass.connection_class_for_self)]
        else
          shard_value
        end
      end

      def or(other)
        super(other.shard(primary_shard))
      end

      private

      def infer_shards_from_primary_key(predicates)
        return unless klass.integral_id?

        primary_key = predicates.detect do |predicate|
          (predicate.is_a?(::Arel::Nodes::Binary) || predicate.is_a?(::Arel::Nodes::HomogeneousIn)) &&
            predicate.left.is_a?(::Arel::Attributes::Attribute) &&
            predicate.left.relation.is_a?(::Arel::Table) && predicate.left.relation.klass == klass &&
            klass.primary_key == predicate.left.name
        end
        return unless primary_key

        right = primary_key.is_a?(::Arel::Nodes::HomogeneousIn) ? primary_key.values : primary_key.right

        case right
        when Array
          id_shards = Set.new
          right.each do |value|
            local_id, id_shard = Shard.local_id_for(value)
            id_shard ||= Shard.current(klass.connection_class_for_self) if local_id
            id_shards << id_shard if id_shard
          end
          return if id_shards.empty?

          if id_shards.length == 1
            id_shard = id_shards.first
          # prefer to not change the shard
          elsif id_shards.include?(primary_shard)
            id_shards.delete(primary_shard)
            self.shard_value = [primary_shard] + id_shards.to_a
            return
          else
            id_shards = id_shards.to_a
            transpose_predicates(nil, primary_shard, id_shards.first)
            self.shard_value = id_shards
            return
          end
        when ::Arel::Nodes::BindParam
          local_id, id_shard = Shard.local_id_for(right.value.value_before_type_cast)
          id_shard ||= Shard.current(klass.connection_class_for_self) if local_id
        when ::ActiveModel::Attribute
          local_id, id_shard = Shard.local_id_for(right.value_before_type_cast)
          id_shard ||= Shard.current(klass.connection_class_for_self) if local_id
        else
          local_id, id_shard = Shard.local_id_for(right)
          id_shard ||= Shard.current(klass.connection_class_for_self) if local_id
        end

        return if !id_shard || id_shard == primary_shard

        transpose_predicates(nil, primary_shard, id_shard)
        self.shard_value = id_shard
      end

      def transposable_attribute_type(relation, column)
        if sharded_primary_key?(relation, column)
          :primary
        elsif sharded_foreign_key?(relation, column)
          :foreign
        end
      end

      def models_for_table(table_name)
        @@models_for_table ||= {}
        @@models_for_table[table_name] ||= ::ActiveRecord::Base.descendants.select { |d| d.table_name == table_name }
      end

      def sharded_foreign_key?(relation, column)
        models_for_table(relation.table_name).any? { |m| m.sharded_column?(column) }
      end

      def sharded_primary_key?(relation, column)
        column = column.to_s
        return column == 'id' if relation.klass == ::ActiveRecord::Base

        relation.klass.primary_key == column && relation.klass.integral_id?
      end

      def source_shard_for_foreign_key(relation, column)
        reflection = nil
        models_for_table(relation.table_name).each do |model|
          reflection = model.send(:reflection_for_integer_attribute, column)
          break if reflection
        end
        return Shard.current(klass.connection_class_for_self) if reflection.options[:polymorphic]

        Shard.current(reflection.klass.connection_class_for_self)
      end

      def relation_and_column(attribute)
        column = attribute.name
        attribute = attribute.relation if attribute.relation.is_a?(::Arel::Nodes::TableAlias)
        [attribute.relation, column]
      end

      def build_where_clause(opts, rest = [])
        opts = sanitize_forbidden_attributes(opts)

        case opts
        when String, Array
          values = Hash === rest.first ? rest.first.values : rest

          values.grep(ActiveRecord::Relation) do |rel|
            # serialize subqueries against the same shard as the outer query is currently
            # targeted to run against
            rel.shard!(primary_shard) if rel.shard_source_value == :implicit && rel.primary_shard != primary_shard
          end

          super
        when Hash, ::Arel::Nodes::Node
          where_clause = super

          predicates = where_clause.send(:predicates)
          infer_shards_from_primary_key(predicates) if shard_source_value == :implicit && shard_value.is_a?(Shard)
          predicates = transpose_predicates(predicates, nil, primary_shard)
          where_clause.instance_variable_set(:@predicates, predicates)
          where_clause
        else
          super
        end
      end

      def arel_columns(columns)
        connection.with_local_table_name { super }
      end

      def arel_column(columns)
        connection.with_local_table_name { super }
      end

      def table_name_matches?(from)
        connection.with_global_table_name { super }
      end

      def each_predicate(predicates = nil, &block)
        return predicates.map(&block) if predicates

        each_predicate_cb(:having_clause, :having_clause=, &block)
        each_predicate_cb(:where_clause, :where_clause=, &block)
      end

      def each_predicate_cb(clause_getter, clause_setter, &block)
        old_clause = send(clause_getter)
        old_predicates = old_clause.send(:predicates)
        return if old_predicates.empty?

        new_predicates = old_predicates.map(&block)
        return if new_predicates == old_predicates

        new_clause = old_clause.dup
        new_clause.instance_variable_set(:@predicates, new_predicates)

        send(clause_setter, new_clause)
      end

      def each_transposable_predicate(predicates = nil, &block)
        each_predicate(predicates) do |predicate|
          if predicate.is_a?(::Arel::Nodes::Grouping)
            next predicate unless predicate.expr.is_a?(::Arel::Nodes::Or)

            or_expr = predicate.expr
            old_left = or_expr.left
            old_right = or_expr.right
            new_left, new_right = each_transposable_predicate([old_left, old_right], &block)

            next predicate if new_left == old_left && new_right == old_right

            next predicate.class.new predicate.expr.class.new(new_left, new_right)
          end

          next predicate unless predicate.is_a?(::Arel::Nodes::Binary) || predicate.is_a?(::Arel::Nodes::HomogeneousIn)
          next predicate unless predicate.left.is_a?(::Arel::Attributes::Attribute)

          relation, column = relation_and_column(predicate.left)
          next predicate unless (type = transposable_attribute_type(relation, column))

          yield(predicate, relation, column, type)
        end
      end

      def each_transposable_predicate_value(predicates = nil)
        each_transposable_predicate(predicates) do |predicate, relation, column, type|
          each_transposable_predicate_value_cb(predicate) do |value|
            yield(value, predicate, relation, column, type)
          end
        end
      end

      def each_transposable_predicate_value_cb(predicate)
        right = if predicate.is_a?(::Arel::Nodes::HomogeneousIn)
                  predicate.values
                else
                  predicate.right
                end

        new_right_value =
          case right
          when Array
            right.map { |val| yield(val).presence }.compact
          when ::ActiveModel::Attribute
            old_value = right.value_before_type_cast
            new_value = yield(old_value)

            if new_value == old_value
              right
            else
              right.class.new(right.name, new_value, right.type)
            end
          when ::Arel::Nodes::BindParam
            old_parent_value = right.value
            old_value = old_parent_value.value_before_type_cast
            new_value = yield(old_value)

            if new_value == old_value
              right
            else
              right.class.new(right.value.class.new(old_parent_value.name, new_value, old_parent_value.type))
            end
          else
            yield(right)
          end

        if new_right_value == right
          predicate
        elsif predicate.right.is_a?(::Arel::Nodes::Casted)
          if new_right_value == right.value
            predicate
          else
            predicate.class.new(predicate.left, right.class.new(new_right_value, right.attribute))
          end
        elsif predicate.is_a?(::Arel::Nodes::HomogeneousIn)
          # switch to a regular In, so that Relation::WhereClause#contradiction? knows about it
          if new_right_value.empty?
            klass = predicate.type == :in ? ::Arel::Nodes::In : ::Arel::Nodes::NotIn
            klass.new(predicate.attribute, new_right_value)
          else
            predicate.class.new(new_right_value, predicate.attribute, predicate.type)
          end
        else
          predicate.class.new(predicate.left, new_right_value)
        end
      end

      def transpose_predicates(predicates,
                               source_shard,
                               target_shard,
                               remove_nonlocal_primary_keys: false)
        each_transposable_predicate_value(predicates) do |value, predicate, relation, column, type|
          remove = true if type == :primary &&
                           remove_nonlocal_primary_keys &&
                           predicate.left.relation.klass == klass &&
                           (predicate.is_a?(::Arel::Nodes::Equality) || predicate.is_a?(::Arel::Nodes::HomogeneousIn))

          current_source_shard =
            if source_shard
              source_shard
            elsif type == :primary
              Shard.current(klass.connection_class_for_self)
            elsif type == :foreign
              source_shard_for_foreign_key(relation, column)
            end

          transpose_predicate_value(value, current_source_shard, target_shard, type, remove)
        end
      end

      def transpose_predicate_value(value, current_shard, target_shard, attribute_type, remove_non_local_ids)
        if value.is_a?(::ActiveRecord::StatementCache::Substitute)
          value.sharded = true # mark for transposition later
          value.primary = true if attribute_type == :primary
          value
        else
          local_id = Shard.relative_id_for(value, current_shard, target_shard) || value
          local_id = [] if remove_non_local_ids && local_id.is_a?(Integer) && local_id > Shard::IDS_PER_SHARD
          local_id
        end
      end
    end
  end
end
