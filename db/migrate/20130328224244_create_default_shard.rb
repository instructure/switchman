class CreateDefaultShard < ActiveRecord::Migration
  def up
    unless Switchman::Shard.default.is_a?(Switchman::Shard)
      Switchman::Shard.reset_column_information
      Switchman::Shard.create!(:default => true)
      Switchman::Shard.default(true)
    end
  end
end
