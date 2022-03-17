# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module ModelSchema
      module ClassMethods
        def quoted_table_name
          @quoted_table_name ||= {}
          @quoted_table_name[Shard.current(connection_class_for_self).id] ||= connection.quote_table_name(table_name)
        end
      end
    end
  end
end
