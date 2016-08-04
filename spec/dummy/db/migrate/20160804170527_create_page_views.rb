class CreatePageViews < ActiveRecord::Migration
  def change
    create_table :page_views, id: false do |t|
      t.string :request_id
    end
  end
end
