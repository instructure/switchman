class MirrorUser < ActiveRecord::Base
  self.shard_category = :mirror_universe

  has_one :user
  belongs_to :belongs_to_user, class_name: :User
end
