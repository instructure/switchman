module Switchman
  module ActiveRecord
    module Association
      def self.included(klass)
        %w{build_record load_target scoped}.each do |method|
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

      def scoped_with_sharding
        shard_value = @reflection.options[:multishard] ? @owner : self.shard
        @owner.shard.activate { scoped_without_sharding.shard(shard_value, :association) }
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
      module Association
        def self.included(klass)
          klass.descendants.each{|d| d.valid_options += [:multishard]}
        end
      end
    end

    module Preloader
      module Association
        def self.included(klass)
          klass.send(:remove_method, :associated_records_by_owner)
          klass.send(:remove_method, :owners_by_key)
          klass.send(:remove_method, :scoped)
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

        def scoped
          build_scope
        end
      end
    end

    module CollectionProxy
      def shard(*args)
        scoped.shard(*args)
      end
    end
  end
end

