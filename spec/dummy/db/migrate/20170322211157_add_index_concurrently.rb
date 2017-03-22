class AddIndexConcurrently < ActiveRecord::Migration
  disable_ddl_transaction!

  def change
    options = { algorithm: :concurrently } if connection.adapter_name == 'PostgreSQL'
    add_index :users, :name, options || {}
  end
end
