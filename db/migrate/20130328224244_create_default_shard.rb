class CreateDefaultShard < ActiveRecord::Migration[4.2]
  def up
    unless Switchman::Shard.default.is_a?(Switchman::Shard)
      Switchman::Shard.reset_column_information
      Switchman::Shard.create!(:default => true)
      Switchman::Shard.default(true)
    end
  end
end
