class CreateDigits < ActiveRecord::Migration
  def change
    create_table :digits do |t|
      t.integer :appendage_id

      t.timestamps
    end
  end
end
