class CreateFeatures < ActiveRecord::Migration
  def change
    create_table :features do |t|
      t.integer :owner_id, :limit => 8
      t.string :owner_type

      t.integer :value

      t.timestamps :null => true
    end
  end
end
