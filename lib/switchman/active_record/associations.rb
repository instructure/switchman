# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Associations
      module Association
        def shard
          reflection.shard(owner)
        end

        def build_record(*args)
          shard.activate { super }
        end

        def load_target
          shard.activate { super }
        end

        def scope
          shard_value = @reflection.options[:multishard] ? @owner : shard
          @owner.shard.activate { super.shard(shard_value, :association) }
        end
      end

      module CollectionAssociation
        def find_target(async: false)
          shards = if reflection.options[:multishard] && owner.respond_to?(:associated_shards)
                     owner.associated_shards
                   else
                     [shard]
                   end
          # activate both the owner and the target's shard category, so that Reflection#join_id_for,
          # when called for the owner, will be returned relative to shard the query will execute on
          Shard.with_each_shard(shards,
                                [klass.connection_class_for_self, owner.class.connection_class_for_self].uniq) do
            if reflection.options[:multishard] && owner.respond_to?(:associated_shards) && reflection.has_scope?
              # Prevent duplicate results when reflection has a scope (when it would use the skip_statement_cache? path)
              # otherwise, the super call will set the shard_value to the object, causing it to iterate too many times
              # over the associated shards
              scope.shard(Shard.current(scope.klass.connection_class_for_self), :association).to_a
            elsif ::Rails.version < "8.0"
              super()
            else
              super
            end
          end
        end

        def _create_record(*)
          shard.activate { super }
        end
      end

      module BelongsToAssociation
        def replace_keys(record, force: false)
          if record&.class&.sharded_column?(reflection.association_primary_key(record.class))
            foreign_id = record[reflection.association_primary_key(record.class)]
            owner[reflection.foreign_key] = Shard.relative_id_for(foreign_id, record.shard, owner.shard)
          else
            super
          end
        end

        def shard
          if @owner.class.sharded_column?(@reflection.foreign_key) &&
             (foreign_id = @owner[@reflection.foreign_key])
            Shard.shard_for(foreign_id, @owner.loaded_from_shard)
          else
            super
          end
        end
      end

      module ForeignAssociation
        # significant change:
        #   * transpose the key to the correct shard
        def set_owner_attributes(record) # rubocop:disable Naming/AccessorMethodName
          return if options[:through]

          key = owner._read_attribute(reflection.join_foreign_key)
          key = Shard.relative_id_for(key, owner.shard, shard)
          record._write_attribute(reflection.join_primary_key, key)

          record._write_attribute(reflection.type, owner.class.polymorphic_name) if reflection.type
        end
      end

      module Extension
        def self.build(_model, _reflection); end

        def self.valid_options
          [:multishard]
        end
      end

      ::ActiveRecord::Associations::Builder::Association.extensions << Extension

      module Preloader
        module Association
          # significant changes:
          #  * associate shards with records
          #  * look on all appropriate shards when loading records
          module LoaderRecords
            def populate_keys_to_load_and_already_loaded_records
              @sharded_keys_to_load = {}

              loaders.each do |loader|
                multishard = loader.send(:reflection).options[:multishard]
                belongs_to = loader.send(:reflection).macro == :belongs_to
                loader.owners_by_key.each do |key, owners|
                  if (loaded_owner = owners.find { |owner| loader.loaded?(owner) })
                    already_loaded_records_by_key[key] = loader.target_for(loaded_owner)
                  else
                    shard_set = @sharded_keys_to_load[key] ||= Set.new
                    owner_key_name = loader.send(:owner_key_name)
                    owners.each do |owner|
                      if multishard && owner.respond_to?(:associated_shards)
                        shard_set.merge(owner.associated_shards.map(&:id))
                      elsif belongs_to && owner.class.sharded_column?(owner_key_name)
                        shard_set.add(Shard.shard_for(owner[owner_key_name], owner.shard).id)
                      elsif belongs_to
                        shard_set.add(Shard.current.id)
                      else
                        shard_set.add(owner.shard.id)
                      end
                    end
                  end
                end
              end

              @sharded_keys_to_load.delete_if { |key, _shards| already_loaded_records_by_key.include?(key) }
            end

            def load_records
              ret = []

              shards_with_keys = @sharded_keys_to_load.each_with_object({}) do |(key, shards), h|
                shards.each { |shard| (h[shard] ||= []) << key }
              end

              shards_with_keys.each do |shard, keys|
                Shard.lookup(shard).activate do
                  scope_was = loader_query.scope
                  begin
                    loader_query.instance_variable_set(
                      :@scope,
                      loader_query.scope.shard(
                        Shard.current(loader_query.scope.model.connection_class_for_self)
                      )
                    )
                    ret += loader_query.load_records_for_keys(keys) do |record|
                      loaders.each { |l| l.set_inverse(record) }
                    end
                  ensure
                    loader_query.instance_variable_set(:@scope, scope_was)
                  end
                end
              end

              ret
            end
          end

          # Copypasta from Activerecord but with added global_id_for goodness.
          def records_for(ids)
            scope.where(association_key_name => ids).load do |record|
              global_key = if model.connection_class_for_self == UnshardedRecord
                             convert_key(record[association_key_name])
                           else
                             Shard.global_id_for(record[association_key_name], record.shard)
                           end
              owner = owners_by_key[convert_key(global_key)].first
              association = owner.association(reflection.name)
              association.set_inverse_instance(record)
            end
          end

          # Disabling to keep closer to rails original
          # rubocop:disable Naming/AccessorMethodName
          # significant changes:
          #  * globalize the key to lookup
          def set_inverse(record)
            global_key = if model.connection_class_for_self == UnshardedRecord
                           convert_key(record[association_key_name])
                         else
                           Shard.global_id_for(record[association_key_name], record.shard)
                         end

            if (owners = owners_by_key[convert_key(global_key)])
              # Processing only the first owner
              # because the record is modified but not an owner
              association = owners.first.association(reflection.name)
              association.set_inverse_instance(record)
            end
          end
          # rubocop:enable Naming/AccessorMethodName

          # significant changes:
          #  * partition_by_shard the records_for call
          #  * re-globalize the fetched owner id before looking up in the map
          # TODO: the ignored param currently loads records; we should probably not waste effort double-loading them
          # Change introduced here: https://github.com/rails/rails/commit/c6c0b2e8af64509b699b782aadfecaa430700ece
          def load_records(raw_records = nil)
            # owners can be duplicated when a relation has a collection association join
            # #compare_by_identity makes such owners different hash keys
            @records_by_owner = {}.compare_by_identity

            raw_records ||= loader_query.records_for([self])

            @preloaded_records = raw_records.select do |record|
              assignments = false

              owner_key = record[association_key_name]
              if owner_key && record.class.sharded_column?(association_key_name)
                owner_key = Shard.global_id_for(owner_key,
                                                record.shard)
              end

              owners_by_key[convert_key(owner_key)]&.each do |owner|
                entries = (@records_by_owner[owner] ||= [])

                if reflection.collection? || entries.empty?
                  entries << record
                  assignments = true
                end
              end

              assignments
            end
          end

          # significant change: globalize keys on sharded columns
          def owners_by_key
            @owners_by_key ||= owners.each_with_object({}) do |owner, result|
              key = owner[owner_key_name]
              key = Shard.global_id_for(key, owner.shard) if key && owner.class.sharded_column?(owner_key_name)
              key = convert_key(key)
              (result[key] ||= []) << owner if key
            end
          end

          # significant change: don't cache scope (since it could be for different shards)
          def scope
            build_scope
          end
        end
      end

      module CollectionProxy
        def initialize(*args)
          super
          self.shard_value = scope.shard_value
          self.shard_source_value = :association
        end

        def shard(*)
          scope.shard(*)
        end
      end

      module AutosaveAssociation
        if ::Rails.version < "7.1"
          def association_foreign_key_changed?(reflection, record, key)
            return false if reflection.through_reflection?

            # have to use send instead of _read_attribute because sharding
            record.has_attribute?(reflection.foreign_key) && record.send(reflection.foreign_key) != key
          end

          def save_belongs_to_association(reflection)
            # this seems counter-intuitive, but the autosave code will assign to attribute bypassing switchman,
            # after reading the id attribute _without_ bypassing switchman. So we need Shard.current for the
            # category of the associated record to match Shard.current for the category of self
            shard.activate(connection_class_for_self_for_reflection(reflection)) { super }
          end
        else
          def association_foreign_key_changed?(reflection, record, key)
            return false if reflection.through_reflection?

            foreign_key = Array(reflection.foreign_key)
            return false unless foreign_key.all? { |k| record._has_attribute?(k) }

            # have to use send instead of _read_attribute because sharding
            foreign_key.map { |k| record.send(k) } != Array(key)
          end

          def save_belongs_to_association(reflection)
            association = association_instance_get(reflection.name)
            return unless association&.loaded? && !association.stale_target?

            record = association.load_target
            return unless record && !record.destroyed?

            autosave = reflection.options[:autosave]

            if autosave && record.marked_for_destruction?
              foreign_key = Array(reflection.foreign_key)
              foreign_key.each { |key| self[key] = nil }
              record.destroy
            elsif autosave != false
              if record.new_record? || (autosave && record.changed_for_autosave?)
                saved = record.save(validate: !autosave)
              end

              if association.updated?
                primary_key = Array(compute_primary_key(reflection, record)).map(&:to_s)
                foreign_key = Array(reflection.foreign_key)

                primary_key_foreign_key_pairs = primary_key.zip(foreign_key)
                primary_key_foreign_key_pairs.each do |pk, fk|
                  # Notable change: add relative_id_for here
                  association_id = if record.class.sharded_column?(pk)
                                     Shard.relative_id_for(
                                       record._read_attribute(pk),
                                       record.shard,
                                       shard
                                     )
                                   else
                                     record._read_attribute(pk)
                                   end
                  self[fk] = association_id unless self[fk] == association_id
                end
                association.loaded!
              end

              saved if autosave
            end
          end
        end
      end
    end
  end
end
