module Switchman
  module ActiveRecord
    module TypeCaster
      module Map
        def model
          @types
        end
      end

      module Connection
        def model
          @klass
        end
      end
    end
  end
end
