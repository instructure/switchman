class CreateFaces < ActiveRecord::Migration
  def change
    create_table :faces do |t|
      t.integer :user_id, :limit => 8

      t.timestamps :null => true
    end
  end
end
