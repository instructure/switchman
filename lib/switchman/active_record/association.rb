module Switchman
  module ActiveRecord
    module Association
      def self.included(klass)
        %w{build_record creation_attributes load_target scope}.each do |method|
          method = 'scoped' if method == 'scope' && ::Rails.version < '4'
          klass.alias_method_chain(method, :sharding)
        end
      end

      def shard
        if @reflection.options[:polymorphic] || @reflection.klass.shard_category == @owner.class.shard_category
          # polymorphic associations assume the same shard as the owning item
          @owner.shard
        else
          Shard.default
        end
      end

      def build_record_with_sharding(*args)
        self.shard.activate { build_record_without_sharding(*args) }
      end

      def load_target_with_sharding
        self.shard.activate { load_target_without_sharding }
      end

      # scoped is renamed to scope in Rails 4
      method = ::Rails.version < '4' ? 'scoped' : 'scope'
      class_eval <<-RUBY, __FILE__, __LINE__ + 1
        def #{method}_with_sharding
          shard_value = @reflection.options[:multishard] ? @owner : self.shard
          @owner.shard.activate { #{method}_without_sharding.shard(shard_value, :association) }
        end
      RUBY

      def creation_attributes_with_sharding
        attributes = creation_attributes_without_sharding

        # translate keys
        if reflection.macro.in?([:has_one, :has_many]) && !options[:through]
          attributes[reflection.foreign_key] = Shard.relative_id_for(owner[reflection.active_record_primary_key], owner.shard, self.shard)
        end
        attributes
      end
    end

    module BelongsToAssociation
      def self.included(klass)
        klass.send(:alias_method_chain, :replace_keys, :sharding)
      end

      def replace_keys_with_sharding(record)
        if record && record.class.sharded_column?(reflection.association_primary_key(record.class))
          foreign_id = record[reflection.association_primary_key(record.class)]
          owner[reflection.foreign_key] = Shard.relative_id_for(foreign_id, record.shard, owner.shard)
        else
          replace_keys_without_sharding(record)
        end
      end

      def shard
        if @owner.class.sharded_column?(@reflection.foreign_key) &&
            foreign_id = @owner[@reflection.foreign_key]
          Shard.shard_for(foreign_id, @owner.shard)
        else
          super
        end
      end
    end

    module Builder
      module CollectionAssociation
        def self.included(klass)
          if ::Rails.version < '4'
            [klass] + klass.descendants.each do |k|
              k.valid_options << :multishard
            end
          end
        end

        def valid_options
          super + [:multishard]
        end
      end
    end

    module Preloader
      module Association
        def self.included(klass)
          klass.send(:remove_method, :associated_records_by_owner)
          klass.send(:remove_method, :owners_by_key)
          if ::Rails.version < '4'
            klass.send(:remove_method, :scoped)
          else
            klass.send(:remove_method, :scope)
          end
        end

        def associated_records_by_owner
          owners_map = owners_by_key

          if klass.nil? || owners_map.empty?
            records = []
          else
            # Some databases impose a limit on the number of ids in a list (in Oracle it's 1000)
            # Make several smaller queries if necessary or make one query if the adapter supports it
            records = Shard.partition_by_shard(owners) do |partitioned_owners|
              sliced_owners = partitioned_owners.each_slice(model.connection.in_clause_length || partitioned_owners.size)
              sliced_owners.map do |slice|
                relative_owner_keys = slice.map do |owner|
                  key = owner[owner_key_name]
                  if key && owner.class.sharded_column?(owner_key_name)
                    key = Shard.relative_id_for(key, owner.shard, Shard.current(owner.class.shard_category))
                  end
                  key && key.to_s
                end
                relative_owner_keys.compact!
                records_for(relative_owner_keys)
              end
            end
            records.flatten!
          end

          # Each record may have multiple owners, and vice-versa
          records_by_owner = Hash[owners.map { |owner| [owner, []] }]
          records.each do |record|
            owner_key = record[association_key_name]
            owner_key = Shard.global_id_for(owner_key, record.shard) if owner_key && record.class.sharded_column?(association_key_name)

            owners_map[owner_key.to_s].each do |owner|
              records_by_owner[owner] << record
            end
          end
          records_by_owner
        end

        def owners_by_key
          @owners_by_key ||= owners.group_by do |owner|
            key = owner[owner_key_name]
            key = Shard.global_id_for(key, owner.shard) if key && owner.class.sharded_column?(owner_key_name)
            key && key.to_s
          end
        end

        def scope
          build_scope
        end
        # renamed to just scope in Rails 4
        if ::Rails.version < '4'
          alias_method :scoped, :scope
          remove_method(:scope)
        end
      end
    end

    module CollectionProxy
      def shard(*args)
        if ::Rails.version < '4'
          scoped.shard(*args)
        else
          scope.shard(*args)
        end
      end
    end
  end
end

