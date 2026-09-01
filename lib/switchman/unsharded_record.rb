# frozen_string_literal: true

module Switchman
  class UnshardedRecord < ::ActiveRecord::Base # rubocop:disable Rails/ApplicationRecord
    self.abstract_class = true
  end
end
