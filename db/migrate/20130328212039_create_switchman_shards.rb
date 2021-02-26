# frozen_string_literal: true

class CreateSwitchmanShards < ActiveRecord::Migration[4.2]
  def change
    create_table :switchman_shards do |t|
      t.string :name
      t.string :database_server_id
      t.boolean :default, default: false, null: false
    end
  end
end
