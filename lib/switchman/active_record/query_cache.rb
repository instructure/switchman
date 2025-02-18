# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module QueryCache
      private

      if ::Rails.version < "7.1"
        def cache_sql(sql, name, binds)
          # have to include the shard id in the cache key because of switching dbs on the same connection
          sql = "#{shard.id}::#{sql}"
          @lock.synchronize do
            result =
              if query_cache[sql].key?(binds)
                args = {
                  sql:,
                  binds:,
                  name:,
                  connection_id: object_id,
                  cached: true,
                  type_casted_binds: -> { type_casted_binds(binds) }
                }
                ::ActiveSupport::Notifications.instrument(
                  "sql.active_record",
                  args
                )
                query_cache[sql][binds]
              else
                query_cache[sql][binds] = yield
              end
            result.dup
          end
        end
      else
        def cache_sql(sql, name, binds)
          # have to include the shard id in the cache key because of switching dbs on the same connection
          sql = "#{shard.id}::#{sql}"
          key = binds.empty? ? sql : [sql, binds]
          result = nil
          hit = false

          @lock.synchronize do
            if ::Rails.version < "7.2"
              if (result = @query_cache.delete(key))
                hit = true
                @query_cache[key] = result
              else
                result = @query_cache[key] = yield
                @query_cache.shift if @query_cache_max_size && @query_cache.size > @query_cache_max_size
              end
            else
              hit = true
              result = @query_cache.compute_if_absent(key) do
                hit = false
                yield
              end
            end
          end

          if hit
            ::ActiveSupport::Notifications.instrument(
              "sql.active_record",
              cache_notification_info(sql, name, binds)
            )
          end

          result.dup
        end
      end
    end
  end
end
