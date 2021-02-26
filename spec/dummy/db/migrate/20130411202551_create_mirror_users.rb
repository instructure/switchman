# frozen_string_literal: true

class CreateMirrorUsers < ActiveRecord::Migration[4.2]
  def change
    create_table :mirror_users do |t|
      t.integer :user_id, limit: 8

      t.timestamps null: true
    end
  end
end
