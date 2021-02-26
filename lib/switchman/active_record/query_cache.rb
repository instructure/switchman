# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module QueryCache
      private

      def cache_sql(sql, name, binds)
        # have to include the shard id in the cache key because of switching dbs on the same connection
        sql = "#{shard.id}::#{sql}"
        @lock.synchronize do
          result =
            if query_cache[sql].key?(binds)
              args = {
                sql: sql,
                binds: binds,
                name: name,
                connection_id: object_id,
                cached: true,
                type_casted_binds: -> { type_casted_binds(binds) }
              }
              ::ActiveSupport::Notifications.instrument(
                'sql.active_record',
                args
              )
              query_cache[sql][binds]
            else
              query_cache[sql][binds] = yield
            end
          result.dup
        end
      end
    end
  end
end
