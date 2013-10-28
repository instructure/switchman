class CreateAppendages < ActiveRecord::Migration
  def change
    create_table :appendages do |t|
      t.integer :user_id
      t.integer :value

      t.timestamps
    end
  end
end
