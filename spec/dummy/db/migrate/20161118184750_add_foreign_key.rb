# frozen_string_literal: true

class AddForeignKey < ActiveRecord::Migration[4.2]
  def change
    add_foreign_key :users, :users, column: :parent_id
  end
end
