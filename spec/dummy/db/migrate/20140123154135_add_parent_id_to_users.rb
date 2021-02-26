# frozen_string_literal: true

class AddParentIdToUsers < ActiveRecord::Migration[4.2]
  def change
    add_column :users, :parent_id, :integer, limit: 8
  end
end
