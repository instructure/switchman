class AddUserIndexes < ActiveRecord::Migration[4.2]
  def change
    add_index :users, :mirror_user_id
    add_index :mirror_users, :user_id
  end
end
