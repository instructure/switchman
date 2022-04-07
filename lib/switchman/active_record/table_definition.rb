# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module TableDefinition
      def column(name, type, limit: nil, **)
        Switchman.foreign_key_check(name, type, limit: limit)
        super
      end
    end
  end
end
