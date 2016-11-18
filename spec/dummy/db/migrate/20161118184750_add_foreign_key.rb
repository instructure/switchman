class AddForeignKey < ActiveRecord::Migration
  def change
    add_foreign_key :users, :users, column: :parent_id
  end
end
