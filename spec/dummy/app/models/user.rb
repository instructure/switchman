class User < ActiveRecord::Base
  attr_accessible :name

  has_many :appendages, :multishard => true
  has_many :digits, :through => :appendages, :multishard => true
  belongs_to :mirror_user
end
