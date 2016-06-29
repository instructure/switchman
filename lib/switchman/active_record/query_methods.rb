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
        raise ImmutableRelation if @loaded
        @values[:shard] = value
      end
      def shard_source_value=(value)
        raise ImmutableRelation if @loaded
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

      if ::Rails.version < '5'
        # moved to WhereClauseFactory#build in Rails 5
        def build_where(opts, other = [])
          case opts
          when String, Array
            values = Hash === other.first ? other.first.values : other

            values.grep(ActiveRecord::Relation) do |rel|
              # serialize subqueries against the same shard as the outer query is currently
              # targeted to run against
              if rel.shard_source_value == :implicit && rel.primary_shard != primary_shard
                rel.shard!(primary_shard)
              end
              self.bind_values += rel.bind_values if ::Rails.version < '4.2'
            end

            [@klass.send(:sanitize_sql, other.empty? ? opts : ([opts] + other))]
          when Hash, ::Arel::Nodes::Node
            predicates = super
            infer_shards_from_primary_key(predicates) if shard_source_value == :implicit && shard_value.is_a?(Shard)
            predicates = transpose_predicates(predicates, nil, primary_shard) if shard_source_value != :explicit
            predicates
          else
            super
          end
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

      if ::Rails.version >= '4.2' && ::Rails.version < '5'
        # fixes an issue in Rails 4.2 with `reverse_sql_order` and qualified names
        # where quoted_table_name is called before shard(s) have been activated
        # if there's no ordering
        def reverse_order!
          orders = order_values.uniq
          orders.reject!(&:blank?)
          if orders.empty?
            self.order_values = [arel_table[primary_key].desc]
          else
            self.order_values = reverse_sql_order(orders)
          end
          self
        end
      end

      private

      [:where, :having].each do |type|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def transpose_#{type}_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
            if ::Rails.version >= '5'
              unless (predicates = #{type}_clause.send(:predicates)).empty?
                new_predicates = transpose_predicates(predicates, source_shard,
                                                      target_shard, remove_nonlocal_primary_keys)
                if new_predicates != predicates
                  self.#{type}_clause = #{type}_clause.dup
                  #{type}_clause.instance_variable_set(:@predicates, new_predicates)
                end
              end
            else
              unless #{type}_values.empty?
                self.#{type}_values = transpose_predicates(#{type}_values,
                  source_shard, target_shard, remove_nonlocal_primary_keys)
              end
            end
          end
        RUBY
      end

      def transpose_clauses(source_shard, target_shard, remove_nonlocal_primary_keys = false)
        transpose_where_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
        transpose_having_clauses(source_shard, target_shard, remove_nonlocal_primary_keys)
      end

      def infer_shards_from_primary_key(predicates, binds = nil)
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
            # look for a bind param with a matching column name
            if ::Rails.version >= "5"
              binds ||= where_clause.binds + having_clause.binds
              if binds && bind = binds.detect{|b| b.try(:name).to_s == klass.primary_key.to_s}
                unless bind.value.is_a?(::ActiveRecord::StatementCache::Substitute)
                  local_id, id_shard = Shard.local_id_for(bind.value)
                  id_shard ||= Shard.current(klass.shard_category) if local_id
                end
              end
            else
              if bind_values && idx = bind_values.find_index{|b| b.is_a?(Array) && b.first.try(:name).to_s == klass.primary_key.to_s}
                column, value = bind_values[idx]
                unless ::Rails.version >= '4.2' && value.is_a?(::ActiveRecord::StatementCache::Substitute)
                  local_id, id_shard = Shard.local_id_for(value)
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
        relation.model.primary_key == column
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
        columns.map do |field|
          if (Symbol === field || String === field) && ::Rails.version >= '5' && (klass.has_attribute?(field) || klass.attribute_alias?(field)) && !from_clause.value
            klass.arel_attribute(field, table)
          elsif (Symbol === field || String === field) && ::Rails.version < '5' && columns_hash.key?(field.to_s) && !from_value
            arel_table[field]
          elsif Symbol === field
            # the rest of this is pulled from AR - the only change is from quote_table_name to quote_column_name here
            # otherwise qualified names will add the schema to a column
            connection.quote_column_name(field.to_s)
          else
            field
          end
        end
      end

      # semi-private
      public
      def transpose_predicates(predicates, source_shard, target_shard, remove_nonlocal_primary_keys = false, binds = nil)
        predicates.map do |predicate|
          next predicate unless predicate.is_a?(::Arel::Nodes::Binary)
          next predicate unless predicate.left.is_a?(::Arel::Attributes::Attribute)
          relation, column = relation_and_column(predicate.left)
          next predicate unless (type = transposable_attribute_type(relation, column))

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

          new_right_value = case predicate.right
          when Array
            local_ids = []
            predicate.right.each do |value|
              local_id = Shard.relative_id_for(value, current_source_shard, target_shard)
              unless remove && local_id > Shard::IDS_PER_SHARD
                if ::Rails.version > "4.2" && value.is_a?(::Arel::Nodes::Casted)
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
            if ::Rails.version >= "5"
              binds ||= where_clause.binds + having_clause.binds
              if binds && bind = binds.detect{|b| b.try(:name).to_s == predicate.left.name.to_s}
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
            else
              if bind_values && idx = bind_values.find_index{|b| b.is_a?(Array) && b.first.try(:name).to_s == predicate.left.name.to_s}
                column, value = bind_values[idx]
                if ::Rails.version >= '4.2' && value.is_a?(::ActiveRecord::StatementCache::Substitute)
                  value.sharded = true # mark for transposition later
                  value.primary = true if type == :primary
                else
                  local_id = Shard.relative_id_for(value, current_source_shard, target_shard)
                  local_id = [] if remove && local_id > Shard::IDS_PER_SHARD
                  bind_values[idx] = [column, local_id]
                end
              end
            end
            predicate.right
          else
            local_id = Shard.relative_id_for(predicate.right, current_source_shard, target_shard)
            local_id = [] if remove && local_id.is_a?(Fixnum) && local_id > Shard::IDS_PER_SHARD
            local_id
          end

          if new_right_value == predicate.right
            predicate
          elsif ::Rails.version >= "4.2" && predicate.right.is_a?(::Arel::Nodes::Casted)
            if new_right_value == predicate.right.val
              predicate
            else
              predicate.class.new(predicate.left, predicate.right.class.new(new_right_value, predicate.right.attribute))
            end
          else
            predicate.class.new(predicate.left, new_right_value)
          end
        end
      end
    end
  end
end
