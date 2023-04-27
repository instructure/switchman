# frozen_string_literal: true

module Switchman
  class DefaultShard
    def id
      "default"
    end
    alias_method :cache_key, :id
    def activate(*_classes)
      yield
    end

    def activate!(*classes); end

    def default?
      true
    end

    def primary?
      true
    end

    def relative_id_for(local_id, _target = nil)
      local_id
    end

    def global_id_for(local_id)
      local_id
    end

    def database_server_id
      nil
    end

    def database_server
      DatabaseServer.find(nil)
    end

    def new_record?
      false
    end

    def name
      unless instance_variable_defined?(:@name)
        @name = nil # prevent taking this branch on recursion
        @name = database_server.shard_name(:bootstrap)
      end
      @name
    end

    def description
      ::Rails.env
    end

    # The default's shard is always the default shard
    def shard
      self
    end

    def _dump(_depth)
      ""
    end

    def self._load(_str)
      Shard.default
    end

    def ==(other)
      return true if other.is_a?(DefaultShard) || (other.is_a?(Shard) && other[:default])

      super
    end

    class << self
      def instance
        @instance ||= new
      end

      private :new
    end
  end
end
