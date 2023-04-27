# frozen_string_literal: true

migrations = Rails.root.join("db/migrate")

ActiveRecord::Migrator.migrations_paths << migrations unless ActiveRecord::Migrator.migrations_paths.any? do |p|
  File.expand_path(p).to_s == migrations.to_s
end
