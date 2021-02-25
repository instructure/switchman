# frozen_string_literal: true

module Switchman
  class UnshardedRecord < ::ActiveRecord::Base
    sharded_model
  end
end
