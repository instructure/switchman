class CreateDigits < ActiveRecord::Migration[4.2]
  def change
    create_table :digits do |t|
      t.integer :appendage_id, :limit => 8
      t.integer :value

      t.timestamps :null => true
    end
  end
end
