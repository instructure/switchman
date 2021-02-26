# frozen_string_literal: true

class CreateUsers < ActiveRecord::Migration[4.2]
  def change
    create_table :users do |t|
      t.string :name
      t.integer :mirror_user_id, limit: 8

      t.timestamps null: true
    end
  end
end
