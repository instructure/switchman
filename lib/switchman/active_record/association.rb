module Switchman
  module ActiveRecord
    module Association
      def self.included(klass)
        %w{build_record load_target scoped}.each do |method|
          klass.alias_method_chain(method, :sharding)
        end
      end

      def shard
        # polymorphic associations assume the same shard as the owning item
        if @reflection.options[:polymorphic] || @reflection.klass.shard_category == @owner.class.shard_category
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
        shard = @reflection.options[:multishard] ? @owner : self.shard
        scoped_without_sharding.shard(shard, :association)
      end
    end

    module Builder
      module Association
        def self.included(klass)
          klass.descendants.each{|d| d.valid_options += [:multishard]}
        end
      end
    end
  end
end
