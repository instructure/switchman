module Switchman
  module ActiveRecord
    module PostgreSQLAdapter
      def self.included(klass)
        klass::NATIVE_DATABASE_TYPES[:primary_key] = "bigserial primary key".freeze
      end

      def schemas
        select_values("SELECT * FROM unnest(current_schemas(false))")
      end
    end
  end
end
