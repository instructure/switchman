class AddTypeToAppendages < ActiveRecord::Migration
  def change
    add_column :appendages, :type, :string, null: false, default: 'Appendage'
  end
end
