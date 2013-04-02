module Switchman
  module ActiveRecord
    # needs to be included in the same class as ::ActiveRecord::ConnectionAdapters::QueryCache
    # *after* that module is included
    module QueryCache
      def cache_sql(sql, *args, &block)
        # have to include the shard id in the cache key because of switching dbs on the same connection
        super("#{self.shard.id}::#{sql}", *args, &block)
      end
    end
  end
end
