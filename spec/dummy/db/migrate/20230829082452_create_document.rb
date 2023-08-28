# frozen_string_literal: true

class CreateDocument < ActiveRecord::Migration[6.1]
  def change
    create_table(:documents, id: false, primary_key: :key) do |t|
      t.text :key
      t.text :body
    end
  end
end
