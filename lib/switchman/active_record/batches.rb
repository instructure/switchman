module Switchman
  module ActiveRecord
    module Batches
      def batch_order
        "#{connection.quote_local_table_name(table_name)}.#{quoted_primary_key} ASC"
      end
    end
  end
end