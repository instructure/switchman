class Digit < ActiveRecord::Base
  attr_accessible :appendage, :appendage_id, :value

  belongs_to :appendage
  has_one :user, :through => :appendage

  scope :has_no_value, where(:value => nil)
  scope :has_value, where("digits.value IS NOT NULL")
end
