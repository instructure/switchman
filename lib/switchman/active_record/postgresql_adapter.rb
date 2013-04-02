module Switchman
  module ActiveRecord
    module PostgreSQLAdapter
      def schemas
        select_values("SELECT * FROM unnest(current_schemas(false))")
      end
    end
  end
end
