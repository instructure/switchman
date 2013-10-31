class User < ActiveRecord::Base
  attr_accessible :name

  has_many :appendages, :multishard => true
  has_many :digits, :through => :appendages, :multishard => true
  belongs_to :mirror_user

  attr_writer :associated_shards

  def associated_shards
    @associated_shards || [self.shard]
  end
end
