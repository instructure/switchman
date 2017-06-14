class AddTypeToAppendages < ActiveRecord::Migration[4.2]
  def change
    add_column :appendages, :type, :string, null: false, default: 'Appendage'
  end
end
