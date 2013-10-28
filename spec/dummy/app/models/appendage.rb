class Appendage < ActiveRecord::Base
  belongs_to :user
  has_many :digits
end
