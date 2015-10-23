module Switchman
  module ActiveRecord
    module ModelSchema
      module ClassMethods
        def quoted_table_name
          @quoted_table_name ||= {}
          @quoted_table_name[Shard.current.id] ||= connection.quote_table_name(table_name)
        end
      end
    end
  end
end
