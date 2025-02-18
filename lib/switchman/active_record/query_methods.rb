# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module QueryMethods
      # Use this class to prevent a value from getting transposed across shards
      class NonTransposingValue < SimpleDelegator
        def class
          __getobj__.class
        end

        def is_a?(other)
          return true if other == NonTransposingValue

          __getobj__.is_a?(other)
        end
      end

      # shard_value is one of:
      #   A shard
      #   An array or relation of shards
      #   An AR object (query runs against that object's associated_shards)
      # shard_source_value is one of:
      #   :implicit    - inferred from current shard when relation was created, or primary key where clause
      #   :explicit    - explicit set on the relation
      #   :association - a special value that scopes from associations use to use slightly different logic
      #                  for foreign key transposition
      def shard_value
        @values[:shard]
      end

      def shard_source_value
        @values[:shard_source]
      end

      def shard_value=(value)
        if @loaded
          error_class = if ::Rails.version < "7.2"
                          ::ActiveRecord::ImmutableRelation
                        else
                          ::ActiveRecord::UnmodifiableRelation
                        end

          raise error_class
        end

        @values[:shard] = value
      end

      def shard_source_value=(value)
        if @loaded
          error_class = if ::Rails.version < "7.2"
                          ::ActiveRecord::ImmutableRelation
                        else
                          ::ActiveRecord::UnmodifiableRelation
                        end

          raise error_class
        end

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
        transpose_predicates(nil, old_primary_shard, primary_shard) if old_primary_shard != primary_shard
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

      # use a temp variable so that the new where clause is built before self.where_clause is read,
      # since build_where_clause might mutate self.where_clause
      def where!(opts, *rest)
        new_clause = build_where_clause(opts, rest)
        self.where_clause += new_clause
        self
      end

      protected

      def arel_columns(columns)
        connection.with_local_table_name { super }
      end

      def remove_nonlocal_primary_keys!
        each_transposable_predicate_value do |value, predicate, _relation, _column, type|
          next value unless
            type == :primary &&
            predicate.left.relation.klass == klass &&
            (predicate.is_a?(::Arel::Nodes::Equality) || predicate.is_a?(::Arel::Nodes::HomogeneousIn))

          (value.is_a?(Integer) && value > Shard::IDS_PER_SHARD) ? [] : value
        end
        self
      end

      private

      def infer_shards_from_primary_key(predicates)
        return unless klass.integral_id?

        primary_key = predicates.detect do |predicate|
          (predicate.is_a?(::Arel::Nodes::Equality) ||
           predicate.is_a?(::Arel::Nodes::In) ||
           predicate.is_a?(::Arel::Nodes::HomogeneousIn)) &&
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
        models_for_table(relation.name).any? { |m| m.sharded_column?(column) }
      end

      def sharded_primary_key?(relation, column)
        column = column.to_s
        return column == "id" if relation.klass == ::ActiveRecord::Base

        relation.klass.primary_key == column && relation.klass.integral_id?
      end

      def source_shard_for_foreign_key(relation, column)
        reflection = nil
        models_for_table(relation.name).each do |model|
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
          values = (Hash === rest.first) ? rest.first.values : rest

          if shard_source_value != :explicit && values.grep(ActiveRecord::Relation).first
            raise "Sub-queries are not allowed as simple substitutions; " \
                  "please build your relation with more structured methods so that Switchman is able to introspect it."
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

      def arel_column(columns)
        connection.with_local_table_name { super }
      end

      def table_name_matches?(from)
        if ::Rails.version < "7.2"
          connection.with_global_table_name { super }
        else
          connection.with_global_table_name do
            table_name = Regexp.escape(table.name)
            # INST: adapter_class -> connection
            quoted_table_name = Regexp.escape(connection.quote_table_name(table.name))
            /(?:\A|(?<!FROM)\s)(?:\b#{table_name}\b|#{quoted_table_name})(?!\.)/i.match?(from.to_s)
          end
        end
      end

      unless ::Rails.version < "7.2"
        def order_column(field)
          arel_column(field) do |attr_name|
            if attr_name == "count" && !group_values.empty?
              table[attr_name]
            else
              # INST: adapter_class -> connection
              ::Arel.sql(connection.quote_table_name(attr_name), retryable: true)
            end
          end
        end
      end

      def each_predicate(predicates = nil, &)
        return predicates.map(&) if predicates

        each_predicate_cb(:having_clause, :having_clause=, &)
        each_predicate_cb(:where_clause, :where_clause=, &)
      end

      def each_predicate_cb(clause_getter, clause_setter, &)
        old_clause = send(clause_getter)
        old_predicates = old_clause.send(:predicates)
        return if old_predicates.empty?

        new_predicates = old_predicates.map(&)
        return if new_predicates == old_predicates

        new_clause = old_clause.dup
        new_clause.instance_variable_set(:@predicates, new_predicates)

        send(clause_setter, new_clause)
      end

      def each_transposable_predicate(predicates, &block)
        each_predicate(predicates) do |predicate|
          case predicate
          when ::Arel::Nodes::Grouping
            next predicate unless predicate.expr.is_a?(::Arel::Nodes::Or)

            or_expr = predicate.expr
            old_left = or_expr.left
            old_right = or_expr.right
            new_left, new_right = each_transposable_predicate([old_left, old_right], &block)

            next predicate if new_left == old_left && new_right == old_right

            next predicate.class.new predicate.expr.class.new(new_left, new_right) if ::Rails.version < "7.2"

            next predicate.class.new predicate.expr.class.new([new_left, new_right])

          when ::Arel::Nodes::SelectStatement
            new_cores = predicate.cores.map do |core|
              next core unless core.is_a?(::Arel::Nodes::SelectCore) # just in case something weird is going on

              new_wheres = each_transposable_predicate(core.wheres, &block)
              new_havings = each_transposable_predicate(core.havings, &block)

              next core if core.wheres == new_wheres && core.havings == new_havings

              new_core = core.clone
              new_core.wheres = new_wheres
              new_core.havings = new_havings
              new_core
            end

            next predicate if predicate.cores == new_cores

            new_node = predicate.clone
            new_node.instance_variable_set(:@cores, new_cores)
            next new_node
          when ::Arel::Nodes::Not
            old_value = predicate.expr
            new_value = each_transposable_predicate([old_value], &block).first

            next predicate if old_value == new_value

            next predicate.class.new(new_value)
          when ::Arel::Nodes::Exists
            old_value = predicate.expressions
            new_value = each_transposable_predicate([old_value], &block).first

            next predicate if old_value == new_value

            next predicate.class.new(new_value)
          end

          next predicate unless predicate.is_a?(::Arel::Nodes::Binary) || predicate.is_a?(::Arel::Nodes::HomogeneousIn)
          next predicate unless predicate.left.is_a?(::Arel::Attributes::Attribute)

          relation, column = relation_and_column(predicate.left)
          next predicate unless (type = transposable_attribute_type(relation, column))

          yield(predicate, relation, column, type)
        end
      end

      def each_transposable_predicate_value(predicates = nil, &block)
        each_transposable_predicate(predicates) do |predicate, relation, column, type|
          each_transposable_predicate_value_cb(predicate, block) do |value|
            yield(value, predicate, relation, column, type)
          end
        end
      end

      def each_transposable_predicate_value_cb(node, original_block, &)
        case node
        when Array
          node.filter_map { |val| each_transposable_predicate_value_cb(val, original_block, &).presence }
        when ::ActiveModel::Attribute
          old_value = node.value_before_type_cast
          new_value = each_transposable_predicate_value_cb(old_value, original_block, &)

          (old_value == new_value) ? node : node.class.new(node.name, new_value, node.type)
        when ::Arel::Nodes::And
          old_value = node.children
          new_value = each_transposable_predicate_value_cb(old_value, original_block, &)

          (old_value == new_value) ? node : node.class.new(new_value)
        when ::Arel::Nodes::BindParam
          old_value = node.value
          new_value = each_transposable_predicate_value_cb(old_value, original_block, &)

          (old_value == new_value) ? node : node.class.new(new_value)
        when ::Arel::Nodes::Casted
          old_value = node.value
          new_value = each_transposable_predicate_value_cb(old_value, original_block, &)

          (old_value == new_value) ? node : node.class.new(new_value, node.attribute)
        when ::Arel::Nodes::HomogeneousIn
          old_value = node.values
          new_value = each_transposable_predicate_value_cb(old_value, original_block, &)

          # switch to a regular In, so that Relation::WhereClause#contradiction? knows about it
          if new_value.empty?
            klass = (node.type == :in) ? ::Arel::Nodes::In : ::Arel::Nodes::NotIn
            klass.new(node.attribute, new_value)
          else
            (old_value == new_value) ? node : node.class.new(new_value, node.attribute, node.type)
          end
        when ::Arel::Nodes::Binary
          old_value = node.right
          new_value = each_transposable_predicate_value_cb(old_value, original_block, &)

          (old_value == new_value) ? node : node.class.new(node.left, new_value)
        when ::Arel::Nodes::SelectStatement
          each_transposable_predicate_value([node], &original_block).first
        else
          yield(node)
        end
      end

      def transpose_predicates(predicates,
                               source_shard,
                               target_shard)
        each_transposable_predicate_value(predicates) do |value, _predicate, relation, column, type|
          current_source_shard =
            if source_shard
              source_shard
            elsif type == :primary
              Shard.current(klass.connection_class_for_self)
            elsif type == :foreign
              source_shard_for_foreign_key(relation, column)
            else
              primary_shard
            end

          transpose_predicate_value(value, current_source_shard, target_shard, type)
        end
      end

      def transpose_predicate_value(value, current_shard, target_shard, attribute_type)
        if value.is_a?(NonTransposingValue)
          value
        elsif value.is_a?(::ActiveRecord::StatementCache::Substitute)
          value.sharded = true # mark for transposition later
          value.primary = true if attribute_type == :primary
          value
        else
          Shard.relative_id_for(value, current_shard, target_shard) || value
        end
      end
    end
  end
end
