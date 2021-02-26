# frozen_string_literal: true

class AddTimestampsToShards < ActiveRecord::Migration[4.2]
  disable_ddl_transaction!

  def change
    add_timestamps :switchman_shards, null: true
    now = Time.now.utc
    Switchman::Shard.update_all(updated_at: now, created_at: now) if Switchman::Shard.current.default?
    change_column_null :switchman_shards, :updated_at, false
    change_column_null :switchman_shards, :created_at, false

    return unless Switchman::Shard.current.default?

    Switchman::Shard.connection.schema_cache.clear!
    Switchman::Shard.reset_column_information
    Switchman::Shard.columns
  end
end
