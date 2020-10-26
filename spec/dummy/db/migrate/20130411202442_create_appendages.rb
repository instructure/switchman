# frozen_string_literal: true

class CreateAppendages < ActiveRecord::Migration[4.2]
  def change
    create_table :appendages do |t|
      t.integer :user_id, :limit => 8
      t.integer :value

      t.timestamps :null => true
    end
  end
end
