class Digit < ActiveRecord::Base
  belongs_to :appendage, :required => false
  has_one :user, :through => :appendage

  scope :has_no_value, -> { where(:value => nil) }
  scope :has_value, -> { where("digits.value IS NOT NULL") }
end
