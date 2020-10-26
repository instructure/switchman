# frozen_string_literal: true

class AddIndexConcurrently < ActiveRecord::Migration[4.2]
  disable_ddl_transaction!

  def change
    options = { algorithm: :concurrently } if connection.adapter_name == 'PostgreSQL'
    add_index :users, :name, options || {}
  end
end
