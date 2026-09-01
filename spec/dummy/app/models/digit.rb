# frozen_string_literal: true

class Digit < ApplicationRecord
  belongs_to :appendage, optional: true
  has_one :user, through: :appendage

  scope :has_no_value, -> { where(value: nil) }
  scope :has_value, -> { where.not(digits: { value: nil }) }
end
