class Digit < ActiveRecord::Base
  attr_accessible :value

  belongs_to :appendage

  scope :has_no_value, where(:value => nil)
  scope :has_value, where("digits.value IS NOT NULL")
end
