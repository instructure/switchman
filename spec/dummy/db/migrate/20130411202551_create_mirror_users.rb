class CreateMirrorUsers < ActiveRecord::Migration
  def change
    create_table :mirror_users do |t|
      t.integer :user_id, :limit => 8

      t.timestamps
    end
  end
end
