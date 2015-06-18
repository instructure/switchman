class AddDummyForeignKey < ActiveRecord::Migration
  def change
    add_column :users, :broken_id, :integer, limit: 8
  end
end
