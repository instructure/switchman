require_dependency 'switchman/database_server'

module Switchman
  class DefaultShard
    def id; 'default'; end
    def activate(*categories); yield; end
    def activate!(*categories); end
    def default?; true; end
    def primary?; true; end
    def relative_id_for(local_id, target = nil); local_id; end
    def global_id_for(local_id); local_id; end
    def database_server_id; nil; end
    def database_server; DatabaseServer.find(nil); end
    def name
      unless instance_variable_defined?(:@name)
        @name = nil # prevent taking this branch on recursion
        @name = database_server.shard_name(:bootstrap)
      end
      @name
    end
    def description; ::Rails.env; end
    # The default's shard is always the default shard
    def shard; self; end
    def _dump(depth)
      ''
    end
    def self._load(str)
      Shard.default
    end

    def ==(rhs)
      return true if rhs.is_a?(DefaultShard) || (rhs.is_a?(Shard) && rhs[:default])
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
