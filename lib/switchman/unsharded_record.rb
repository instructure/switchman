# frozen_string_literal: true

module Switchman
  class UnshardedRecord < ::ActiveRecord::Base
    self.abstract_class = true
  end
end
