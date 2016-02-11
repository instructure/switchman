module Switchman
  module ActiveRecord
    module TableDefinition
      def column(name, type, options = {})
        Engine.foreign_key_check(name, type, options)
        super
      end
    end
  end
end
