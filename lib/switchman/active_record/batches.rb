# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module Batches
      def batch_order
        ::Arel.sql("#{connection.quote_local_table_name(table_name)}.#{quoted_primary_key} ASC")
      end
    end
  end
end