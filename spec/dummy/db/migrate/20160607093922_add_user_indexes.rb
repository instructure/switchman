class AddUserIndexes < ActiveRecord::Migration
  def change
    add_index :users, :mirror_user_id
    add_index :mirror_users, :user_id
  end
end
