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
        old_primary_shard = self.primary_shard
        self.shard_value = value
        self.shard_source_value = source
        if (old_primary_shard != self.primary_shard || source == :to_a)
          transpose_clauses(old_primary_shard, self.primary_shard, source == :to_a)
        end
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
          Shard.current(klass.shard_category)
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
          [Shard.current(klass.shard_category)]
        else
          shard_value
        end
      end

      def or(other)
        super(other.shard(self.primary_shard))
      end

      private

      if ::Rails.version >= '5.2'
        [:where, :having].each do |type|
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def transpose_#{type}_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
            unless (predicates = #{type}_clause.send(:predicates)).empty?
              new_predicates, _binds = transpose_predicates(predicates, source_shard,
                                                               target_shard, remove_nonlocal_primary_keys)
              if new_predicates != predicates
                self.#{type}_clause = #{type}_clause.dup
                if new_predicates != predicates
                  #{type}_clause.instance_variable_set(:@predicates, new_predicates)
                end
              end
            end
          end
          RUBY
        end
      else
        [:where, :having].each do |type|
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def transpose_#{type}_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
              unless (predicates = #{type}_clause.send(:predicates)).empty?
                new_predicates, new_binds = transpose_predicates(predicates, source_shard,
                                                                 target_shard, remove_nonlocal_primary_keys,
                                                                 binds: #{type}_clause.binds,
                                                                 dup_binds_on_mutation: true)
                if new_predicates != predicates || !new_binds.equal?(#{type}_clause.binds)
                  self.#{type}_clause = #{type}_clause.dup
                  if new_predicates != predicates
                    #{type}_clause.instance_variable_set(:@predicates, new_predicates)
                  end
                  if !new_binds.equal?(#{type}_clause.binds)
                    #{type}_clause.instance_variable_set(:@binds, new_binds)
                  end
                end
              end
            end
          RUBY
        end
      end

      def transpose_clauses(source_shard, target_shard, remove_nonlocal_primary_keys = false)
        transpose_where_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
        transpose_having_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
      end

      def infer_shards_from_primary_key(predicates, binds = nil)
        return unless klass.integral_id?

        primary_key = predicates.detect do |predicate|
          predicate.is_a?(::Arel::Nodes::Binary) && predicate.left.is_a?(::Arel::Attributes::Attribute) &&
            predicate.left.relation.is_a?(::Arel::Table) && predicate.left.relation.model == klass &&
            klass.primary_key == predicate.left.name
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
              transpose_clauses(primary_shard, id_shards.first)
              self.shard_value = id_shards
              return
            end
          when ::Arel::Nodes::BindParam
            if ::Rails.version >= "5.2"
              local_id, id_shard = Shard.local_id_for(primary_key.right.value.value_before_type_cast)
              id_shard ||= Shard.current(klass.shard_category) if local_id
            else
              # look for a bind param with a matching column name
              if binds && bind = binds.detect{|b| b&.name.to_s == klass.primary_key.to_s}
                unless bind.value.is_a?(::ActiveRecord::StatementCache::Substitute)
                  local_id, id_shard = Shard.local_id_for(bind.value)
                  id_shard ||= Shard.current(klass.shard_category) if local_id
                end
              end
            end
          else
            local_id, id_shard = Shard.local_id_for(primary_key.right)
            id_shard ||= Shard.current(klass.shard_category) if local_id
          end

          return if !id_shard || id_shard == primary_shard
          transpose_clauses(primary_shard, id_shard)
          self.shard_value = id_shard
        end
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
        return column == 'id' if relation.model == ::ActiveRecord::Base
        relation.model.primary_key == column && relation.model.integral_id?
      end

      def source_shard_for_foreign_key(relation, column)
        reflection = nil
        models_for_table(relation.table_name).each do |model|
          reflection = model.send(:reflection_for_integer_attribute, column)
          break if reflection
        end
        return Shard.current(klass.shard_category) if reflection.options[:polymorphic]
        Shard.current(reflection.klass.shard_category)
      end

      def relation_and_column(attribute)
        column = attribute.name
        attribute = attribute.relation if attribute.relation.is_a?(::Arel::Nodes::TableAlias)
        [attribute.relation, column]
      end

      def where_clause_factory
        super.tap { |factory| factory.scope = self }
      end

      def arel_columns(columns)
        connection.with_local_table_name { super }
      end

      def arel_column(columns)
        connection.with_local_table_name { super }
      end

      # semi-private
      public
      def transpose_predicates(predicates,
                               source_shard,
                               target_shard,
                               remove_nonlocal_primary_keys = false,
                               binds: nil,
                               dup_binds_on_mutation: false)
        result = predicates.map do |predicate|
          transposed, binds = transpose_single_predicate(predicate, source_shard, target_shard, remove_nonlocal_primary_keys,
                                     binds: binds, dup_binds_on_mutation: dup_binds_on_mutation)
          transposed
        end
        result = [result, binds]
        result
      end

      def transpose_single_predicate(predicate,
                                     source_shard,
                                     target_shard,
                                     remove_nonlocal_primary_keys = false,
                                     binds: nil,
                                     dup_binds_on_mutation: false)
        if predicate.is_a?(::Arel::Nodes::Grouping)
          return predicate, binds unless predicate.expr.is_a?(::Arel::Nodes::Or)
          # Dang, we have an OR.  OK, that means we have other epxressions below this
          # level, perhaps many, that may need transposition.
          # the left side and right side must each be treated as predicate lists and
          # transformed in kind, if neither of them changes we can just return the grouping as is.
          # hold on, it's about to get recursive...
          #
          # TODO: "binds" is getting passed up and down
          # this stack purely because of the necessary handling for rails <5.2
          #  Dropping support for 5.2 means we can remove the "binds" argument from
          # all of this and yank the conditional below where we monkey with their instance state.
          or_expr = predicate.expr
          left_node = or_expr.left
          right_node = or_expr.right
          left_predicates = left_node.children
          right_predicates = right_node.children
          new_left_predicates, binds = transpose_predicates(left_predicates, source_shard,
                                                               target_shard, remove_nonlocal_primary_keys,
                                                               binds: binds, dup_binds_on_mutation: dup_binds_on_mutation)
          new_right_predicates, binds = transpose_predicates(right_predicates, source_shard,
                                                               target_shard, remove_nonlocal_primary_keys,
                                                               binds: binds, dup_binds_on_mutation: dup_binds_on_mutation)
          if new_left_predicates != left_predicates
            left_node.instance_variable_set(:@children, new_left_predicates)
          end
          if new_right_predicates != right_predicates
            right_node.instance_variable_set(:@children, new_right_predicates)
          end
          return predicate, binds
        end
        return predicate, binds unless predicate.is_a?(::Arel::Nodes::Binary)
        return predicate, binds unless predicate.left.is_a?(::Arel::Attributes::Attribute)
        relation, column = relation_and_column(predicate.left)
        return predicate, binds unless (type = transposable_attribute_type(relation, column))

        remove = true if type == :primary &&
            remove_nonlocal_primary_keys &&
            predicate.left.relation.model == klass &&
            predicate.is_a?(::Arel::Nodes::Equality)

        current_source_shard =
            if source_shard
              source_shard
            elsif type == :primary
              Shard.current(klass.shard_category)
            elsif type == :foreign
              source_shard_for_foreign_key(relation, column)
            end

        if ::Rails.version >= "5.2"
          new_right_value =
            case predicate.right
            when Array
              predicate.right.map {|val| transpose_predicate_value(val, current_source_shard, target_shard, type, remove) }
            else
              transpose_predicate_value(predicate.right, current_source_shard, target_shard, type, remove)
            end
        else
          new_right_value = case predicate.right
          when Array
            local_ids = []
            predicate.right.each do |value|
              local_id = Shard.relative_id_for(value, current_source_shard, target_shard)
              next unless local_id
              unless remove && local_id > Shard::IDS_PER_SHARD
                if value.is_a?(::Arel::Nodes::Casted)
                  if local_id == value.val
                    local_id = value
                  elsif local_id != value
                    local_id = value.class.new(local_id, value.attribute)
                  end
                end
                local_ids << local_id
              end
            end
            local_ids
          when ::Arel::Nodes::BindParam
            # look for a bind param with a matching column name
            if binds && bind = binds.detect{|b| b&.name.to_s == predicate.left.name.to_s}
              # before we mutate, dup
              if dup_binds_on_mutation
                binds = binds.map(&:dup)
                dup_binds_on_mutation = false
                bind = binds.find { |b| b&.name.to_s == predicate.left.name.to_s }
              end
              if bind.value.is_a?(::ActiveRecord::StatementCache::Substitute)
                bind.value.sharded = true # mark for transposition later
                bind.value.primary = true if type == :primary
              else
                local_id = Shard.relative_id_for(bind.value, current_source_shard, target_shard)
                local_id = [] if remove && local_id > Shard::IDS_PER_SHARD
                bind.instance_variable_set(:@value, local_id)
                bind.instance_variable_set(:@value_for_database, nil)
              end
            end
            predicate.right
          else
            local_id = Shard.relative_id_for(predicate.right, current_source_shard, target_shard) || predicate.right
            local_id = [] if remove && local_id.is_a?(Integer) && local_id > Shard::IDS_PER_SHARD
            local_id
          end
        end
        out_predicate = if new_right_value == predicate.right
          predicate
        elsif predicate.right.is_a?(::Arel::Nodes::Casted)
          if new_right_value == predicate.right.val
            predicate
          else
            predicate.class.new(predicate.left, predicate.right.class.new(new_right_value, predicate.right.attribute))
          end
        else
          predicate.class.new(predicate.left, new_right_value)
        end
        return out_predicate, binds
      end

      def transpose_predicate_value(value, current_shard, target_shard, attribute_type, remove_non_local_ids)
        if value.is_a?(::Arel::Nodes::BindParam)
          query_att = value.value
          current_id = query_att.value_before_type_cast
          if current_id.is_a?(::ActiveRecord::StatementCache::Substitute)
            current_id.sharded = true # mark for transposition later
            current_id.primary = true if attribute_type == :primary
            value
          else
            local_id = Shard.relative_id_for(current_id, current_shard, target_shard) || current_id
            local_id = [] if remove_non_local_ids && local_id.is_a?(Integer) && local_id > Shard::IDS_PER_SHARD
            if current_id != local_id
              # make a new bind param
              ::Arel::Nodes::BindParam.new(query_att.class.new(query_att.name, local_id, query_att.type))
            else
              value
            end
          end
        else
          local_id = Shard.relative_id_for(value, current_shard, target_shard) || value
          local_id = [] if remove_non_local_ids && local_id.is_a?(Integer) && local_id > Shard::IDS_PER_SHARD
          local_id
        end
      end
    end
  end
end
