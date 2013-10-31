class Appendage < ActiveRecord::Base
  attr_accessible :user, :user_id, :value

  belongs_to :user
  has_many :digits

  scope :has_no_value, where(:value => nil)
  scope :has_value, where("appendages.value IS NOT NULL")
end
