# frozen_string_literal: true

migrations = Rails.root.join("db/migrate")

unless ActiveRecord::Migrator.migrations_paths.any? { |p| File.expand_path(p).to_s == migrations.to_s }
  ActiveRecord::Migrator.migrations_paths << migrations
end
