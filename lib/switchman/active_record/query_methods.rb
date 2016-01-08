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
      if ::Rails.version < '4'
        attr_accessor :shard_value, :shard_source_value
      else
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
      end

      def shard(value, source = :explicit)
        (::Rails.version < '4' ? clone : spawn).shard!(value, source)
      end

      def shard!(value, source = :explicit)
        raise ArgumentError, "shard can't be nil" unless value
        primary_shard = self.primary_shard
        self.shard_value = value
        self.shard_source_value = source
        if (primary_shard != self.primary_shard || source == :to_a)
          self.where_values = transpose_predicates(where_values, primary_shard, self.primary_shard, source == :to_a) if !where_values.empty?
          self.having_values = transpose_predicates(having_values, primary_shard, self.primary_shard, source == :to_a) if !having_values.empty?
        end
        self
      end

      if ::Rails.version < '4'
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
      end

      def build_where(opts, other = [])
        case opts
        when Hash, ::Arel::Nodes::Node
          predicates = super
          infer_shards_from_primary_key(predicates) if shard_source_value == :implicit && shard_value.is_a?(Shard)
          predicates = transpose_predicates(predicates, nil, primary_shard) if shard_source_value != :explicit
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
        else
          shard_value
        end
      end

      private
      def infer_shards_from_primary_key(predicates)
        primary_key = predicates.detect do |predicate|
          predicate.is_a?(::Arel::Nodes::Binary) && predicate.left.is_a?(::Arel::Attributes::Attribute) &&
            predicate.left.relation.is_a?(::Arel::Table) && predicate.left.relation.engine == klass &&
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
              self.where_values = transpose_predicates(where_values, primary_shard, id_shards.first) if !where_values.empty?
              self.having_values = transpose_predicates(having_values, primary_shard, id_shards.first) if !having_values.empty?
              self.shard_value = id_shards
              return
            end
          when ::Arel::Nodes::BindParam
            # look for a bind param with a matching column name
            if bind_values && idx = bind_values.find_index{|b| b.is_a?(Array) && b.first.try(:name).to_s == klass.primary_key.to_s}
              column, value = bind_values[idx]
              unless ::Rails.version >= '4.2' && value.is_a?(::ActiveRecord::StatementCache::Substitute)
                local_id, id_shard = Shard.local_id_for(value)
                id_shard ||= Shard.current(klass.shard_category) if local_id
              end
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
        return column == 'id' if relation.engine == ::ActiveRecord::Base
        relation.engine.primary_key == column
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

      # semi-private
      public
      def transpose_predicates(predicates, source_shard, target_shard, remove_nonlocal_primary_keys = false)
        predicates.map do |predicate|
          next predicate unless predicate.is_a?(::Arel::Nodes::Binary)
          next predicate unless predicate.left.is_a?(::Arel::Attributes::Attribute)
          relation, column = relation_and_column(predicate.left)
          next predicate unless (type = transposable_attribute_type(relation, column))

          remove = true if type == :primary && remove_nonlocal_primary_keys && predicate.left.relation.engine == klass
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
            predicate.right
          else
            local_id = Shard.relative_id_for(predicate.right, current_source_shard, target_shard)
            local_id = [] if remove && local_id > Shard::IDS_PER_SHARD
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
