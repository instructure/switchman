# frozen_string_literal: true

class AddIndexConcurrently < ActiveRecord::Migration[4.2]
  disable_ddl_transaction!

  def change
    add_index :users, :name, algorithm: :concurrently
  end
end
