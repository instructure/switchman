class CreateMirrorUsers < ActiveRecord::Migration
  def change
    create_table :mirror_users do |t|
      t.integer :user_id

      t.timestamps
    end
  end
end
