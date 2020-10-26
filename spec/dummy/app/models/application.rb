# frozen_string_literal: true

class Application < ActiveRecord::Base
  self.shard_category = :unsharded
  belongs_to :root, :required => false
end
