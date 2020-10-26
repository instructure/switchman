# frozen_string_literal: true

class CreateFaces < ActiveRecord::Migration[4.2]
  def change
    create_table :faces do |t|
      t.integer :user_id, :limit => 8

      t.timestamps :null => true
    end
  end
end
