# frozen_string_literal: true

class CreateFeatures < ActiveRecord::Migration[4.2]
  def change
    create_table :features do |t|
      t.integer :owner_id, limit: 8
      t.string :owner_type

      t.integer :value

      t.timestamps null: true
    end
  end
end
