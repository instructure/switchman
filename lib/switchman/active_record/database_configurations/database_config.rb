# frozen_string_literal: true

module Switchman
  module ActiveRecord
    module DatabaseConfigurations
      module DatabaseConfig
        def for_current_env?
          true
        end
      end
    end
  end
end
