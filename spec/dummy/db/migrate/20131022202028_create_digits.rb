class CreateDigits < ActiveRecord::Migration
  def change
    create_table :digits do |t|
      t.integer :appendage_id, :limit => 8
      t.integer :value

      t.timestamps
    end
  end
end
