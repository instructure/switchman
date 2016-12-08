module Switchman
  module ActiveRecord
    module Relation
      def self.prepended(klass)
        klass::SINGLE_VALUE_METHODS.concat [ :shard, :shard_source ]
      end

      def initialize(*args)
        super
        self.shard_value = Shard.current(klass ? klass.shard_category : :primary) unless shard_value
        self.shard_source_value = :implicit unless shard_source_value
      end

      def clone
        result = super
        result.shard_value = Shard.current(klass ? klass.shard_category : :primary) unless shard_value
        result
      end

      def merge(*args)
        relation = super
        if relation.shard_value != self.shard_value && relation.shard_source_value == :implicit
          relation.shard_value = self.shard_value
          relation.shard_source_value = self.shard_source_value
        end
        relation
      end

      def new(*args, &block)
        primary_shard.activate(klass.shard_category) { super }
      end

      def create(*args, &block)
        primary_shard.activate(klass.shard_category) { super }
      end

      def create!(*args, &block)
        primary_shard.activate(klass.shard_category) { super }
      end

      def to_sql
        primary_shard.activate(klass.shard_category) { super }
      end

      def explain
        self.activate { |relation| relation.call_super(:explain, Relation) }
      end

      to_a_method = ::Rails.version >= '5' ? :records : :to_a
      class_eval <<-RUBY, __FILE__, __LINE__ + 1
        def #{to_a_method}
          return @records if loaded?
          results = self.activate { |relation| relation.call_super(#{to_a_method.inspect}, Relation) }
          case shard_value
          when Array, ::ActiveRecord::Relation, ::ActiveRecord::Base
            @records = results
            @loaded = true
          end
          results
        end
      RUBY

      %I{update_all delete_all}.each do |method|
        class_eval <<-RUBY, __FILE__, __LINE__ + 1
          def #{method}(*args)
            result = self.activate { |relation| relation.call_super(#{method.inspect}, Relation, *args) }
            result = result.sum if result.is_a?(Array)
            result
          end
        RUBY
      end

      def activate(&block)
        shards = all_shards
        if (Array === shards && shards.length == 1)
          if shards.first == DefaultShard || shards.first == Shard.current(klass.shard_category)
            yield(self, shards.first)
          else
            shards.first.activate(klass.shard_category) { yield(self, shards.first) }
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
