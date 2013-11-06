class User < ActiveRecord::Base
  attr_accessible :name

  has_many :appendages, :multishard => true
  has_many :digits, :through => :appendages, :multishard => true
  belongs_to :mirror_user

  attr_writer :associated_shards
  class << self
    attr_accessor :associated_shards_map
  end

  def self.associated_shards_for(global_id)
    associated_shards_map[global_id] if global_id && associated_shards_map
  end

  def associated_shards
    self.class.associated_shards_for(self.global_id) || @associated_shards || [self.shard]
  end
end
