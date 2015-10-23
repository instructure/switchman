module Switchman
  module ActiveRecord
    module Reflection
      module AssociationReflection
        # removes memoization - ActiveRecord::ModelSchema does that anyway;
        # and in fact this is the exact change AR makes in 4.2+
        if ::Rails.version < '4.2'
          def quoted_table_name
            klass.quoted_table_name
          end
        end
      end
    end
  end
end
