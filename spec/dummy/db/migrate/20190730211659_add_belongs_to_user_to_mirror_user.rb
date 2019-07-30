class AddBelongsToUserToMirrorUser < ActiveRecord::Migration[4.2]
  def change
    add_column :mirror_users, :belongs_to_user_id, :integer, limit: 8
  end
end
