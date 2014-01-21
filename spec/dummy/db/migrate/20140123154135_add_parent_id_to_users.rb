class AddParentIdToUsers < ActiveRecord::Migration
  def change
    add_column :users, :parent_id, :integer, :limit => 8
  end
end
