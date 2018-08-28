class AddTimestampsToShards < ActiveRecord::Migration[4.2]
  def change
    add_timestamps :switchman_shards, null: true
    now = Time.now.utc
    Switchman::Shard.update_all(updated_at: now, created_at: now)
    change_column_null :switchman_shards, :updated_at, false
    change_column_null :switchman_shards, :created_at, false
  end
end
