class MirrorUser < ActiveRecord::Base
  self.shard_category = :mirror_universe

  has_one :user
end
