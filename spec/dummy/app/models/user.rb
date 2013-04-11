class User < ActiveRecord::Base
  attr_accessible :name

  has_many :appendages
  belongs_to :mirror_user
end
