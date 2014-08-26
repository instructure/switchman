module Switchman
  module ActiveRecord
    module Relation
      def self.included(klass)
        klass::SINGLE_VALUE_METHODS.concat [ :shard, :shard_source ]

        %w{exec_queries update_all delete_all}.each do |method|
          klass.alias_method_chain(method, :deshackles)
        end

        %w{initialize explain to_a update_all delete_all new create create!}.each do |method|
          klass.alias_method_chain(method, :sharding)
        end
      end

      def initialize_with_sharding(*args)
        initialize_without_sharding(*args)
        self.shard_value = Shard.current(klass.try(:shard_category) || :default) unless shard_value
        self.shard_source_value = :implicit unless shard_source_value
      end

      def merge(*args)
        relation = super
        if relation.shard_value != self.shard_value && relation.shard_source_value == :implicit
          relation.shard_value = self.shard_value
          relation.shard_source_value = self.shard_source_value
        end
        relation
      end

      def new_with_sharding(*args, &block)
        primary_shard.activate(klass.shard_category) { new_without_sharding(*args, &block) }
      end

      def create_with_sharding(*args, &block)
        primary_shard.activate(klass.shard_category) { create_without_sharding(*args, &block) }
      end

      def create_with_sharding!(*args, &block)
        primary_shard.activate(klass.shard_category) { create_without_sharding!(*args, &block) }
      end

      def exec_queries_with_deshackles(*args)
        if self.lock_value
          db = Shard.current(shard_category).database_server
          if ::Shackles.environment != db.shackles_environment
            return db.unshackle { exec_queries_without_deshackles(*args) }
          end
        end
        exec_queries_without_deshackles(*args)
      end

      def explain_with_sharding
        self.activate { |relation| relation.send(:explain_without_sharding) }
      end

      def to_a_with_sharding
        return @records if loaded?
        results = self.activate { |relation| relation.send(:to_a_without_sharding) }
        case shard_value
        when Array, ::ActiveRecord::Relation, ::ActiveRecord::Base
          @records = results
          @loaded = true
        end
        results
      end

      %w{update_all delete_all}.each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}_with_deshackles(*args)
            db = Shard.current(shard_category).database_server
            if ::Shackles.environment != db.shackles_environment
              db.unshackle { #{method}_without_deshackles(*args) }
            else
              #{method}_without_deshackles(*args)
            end
          end

          def #{method}_with_sharding(*args)
            self.activate{|relation| relation.#{method}_without_sharding(*args)}
          end
        RUBY
      end

      def activate(&block)
        shards = all_shards
        if (Array === shards && shards.length == 1)
          if shards.first == DefaultShard || shards.first == Shard.current(klass.shard_category)
            yield(self, shards.first)
          else
            shards.first.activate(klass.shard_category) { yield(self, shard_value) }
          end
        else
          # TODO: implement local limit to avoid querying extra shards
          Shard.with_each_shard(shards, [klass.shard_category]) do
            shard(Shard.current(klass.shard_category), :to_a).activate(&block)
          end
        end
      end
    end
  end
end
