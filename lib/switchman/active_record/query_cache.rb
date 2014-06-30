module Switchman
  module ActiveRecord
    # needs to be included in the same class as ::ActiveRecord::ConnectionAdapters::QueryCache
    # *after* that module is included
    module QueryCache
      # thread local accessors to replace @query_cache_enabled
      def query_cache
        thread_cache = Thread.current[:query_cache] ||= {}
        thread_cache[self.object_id] ||= Hash.new { |h,sql| h[sql] = {} }
      end

      def query_cache_enabled
        Thread.current[:query_cache_enabled]
      end

      def query_cache_enabled=(value)
        Thread.current[:query_cache_enabled] = value
      end

      # basically wholesale repeat of the methods from the original (see
      # https://github.com/rails/rails/blob/master/activerecord/lib/active_record/connection_adapters/abstract/query_cache.rb),
      # but with self.query_cache_enabled and self.query_cache_enabled= instead
      # of @query_cache_enabled.

      def enable_query_cache!
        self.query_cache_enabled = true
      end

      def disable_query_cache!
        self.query_cache_enabled = false
      end

      def cache
        old, self.query_cache_enabled = query_cache_enabled, true
        yield
      ensure
        self.query_cache_enabled = old
        clear_query_cache unless self.query_cache_enabled
      end

      def uncached
        old, self.query_cache_enabled = query_cache_enabled, false
        yield
      ensure
        self.query_cache_enabled = old
      end

      def clear_query_cache
        Thread.current[:query_cache].try(:clear)
      end

      def select_all(arel, name = nil, binds = [])
        if self.query_cache_enabled && !locked?(arel)
          sql = to_sql(arel, binds)
          cache_sql(sql, binds) { super(sql, name, binds) }
        else
          super
        end
      end

      # no reason to define these on the including class directly. the super
      # works just as well from a method on the included module
      [:insert, :update, :delete].each do |method_name|
        class_eval <<-end_code, __FILE__, __LINE__ + 1
          def #{method_name}(*args)
            clear_query_cache if self.query_cache_enabled
            super
          end
        end_code
      end

      private

      def cache_sql(sql, binds)
        # have to include the shard id in the cache key because of switching dbs on the same connection
        sql = "#{self.shard.id}::#{sql}"
        result =
            if query_cache[sql].key?(binds)
              ::ActiveSupport::Notifications.instrument("sql.active_record",
                                                      :sql => sql, :binds => binds, :name => "CACHE", :connection_id => object_id)
              query_cache[sql][binds]
            else
              query_cache[sql][binds] = yield
            end

        result.collect { |row| row.dup }
      end

      def self.included(base)
        base.class_eval do
          # when we call insert, update, and delete, we want it to find the
          # definitions from this module (which will then find the definitions
          # from ActiveRecord::ConnectionAdapters::DatabaseStatements as
          # 'super'), not the ones defined on base by
          # ActiveRecord::ConnectionAdapters::QueryCache.
          remove_method :insert
          remove_method :update
          remove_method :delete
        end
      end
    end
  end
end
