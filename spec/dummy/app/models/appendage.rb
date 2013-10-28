class Appendage < ActiveRecord::Base
  attr_accessible :value

  belongs_to :user
  has_many :digits
end
