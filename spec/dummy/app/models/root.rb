# frozen_string_literal: true

class Root < Switchman::UnshardedRecord
  belongs_to :user, optional: true
end
