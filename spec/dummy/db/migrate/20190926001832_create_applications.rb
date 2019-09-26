class CreateApplications < ActiveRecord::Migration[4.2]
  def change
    create_table :applications do |t|
      t.integer :root_id, limit: 8
    end
  end
end
