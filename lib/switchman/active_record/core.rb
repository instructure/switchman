require "switchman/shard_aware_statement_cache"

module Switchman
  module ActiveRecord
    module Core
      def initialize_find_by_cache
        if ::Rails.version < '5'
          self.find_by_statement_cache = ShardAwareStatementCache.new(shard_category)
        else
          # note that this will not work beyond ActiveRecord 5.0.0.beta3 since
          #  as of beta4 this has been replaced with a hash containing two separate caches:
          #  one for prepared statements, and one for unprepared ones
          @find_by_statement_cache = ShardAwareStatementCache.new(shard_category)
        end
      end
    end
  end
end
