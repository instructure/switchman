if Rails.version < '5'
  # shim to allow Rails 5 style migration version tagging in Rails 4.2 specs
  module ARMigration
    def [](_version)
      self
    end
  end

  ActiveRecord::Migration.singleton_class.prepend(ARMigration)
end