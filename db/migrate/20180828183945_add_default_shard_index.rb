# frozen_string_literal: true

class AddDefaultShardIndex < ActiveRecord::Migration[4.2]
  def change
    Switchman::Shard.where(default: nil).update_all(default: false) if Switchman::Shard.current.default? # rubocop:disable Rails/SkipsModelValidations
    change_column_default :switchman_shards, :default, from: nil, to: false
    change_column_null :switchman_shards, :default, false
    options = if connection.adapter_name == "PostgreSQL"
                { unique: true, where: '"default"' }
              else
                {}
              end
    add_index :switchman_shards, :default, **options
  end
end
