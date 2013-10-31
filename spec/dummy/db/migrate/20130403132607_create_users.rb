class CreateUsers < ActiveRecord::Migration
  def change
    create_table :users do |t|
      t.string :name
      t.integer :mirror_user_id, :limit => 8

      t.timestamps
    end
  end
end
