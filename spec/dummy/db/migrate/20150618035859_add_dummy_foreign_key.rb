class AddDummyForeignKey < ActiveRecord::Migration[4.2]
  def change
    add_column :users, :broken_id, :integer, limit: 8
  end
end
