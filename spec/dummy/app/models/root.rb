class Root < ActiveRecord::Base
  self.shard_category = :unsharded

  belongs_to :user, :required => false
end