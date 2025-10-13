# frozen_string_literal: true

source "http://rubygems.org"

plugin "bundler-multilock", "1.3.4"
return unless Plugin.installed?("bundler-multilock")

Plugin.send(:load_plugin, "bundler-multilock")

gemspec

lockfile "activerecord-7.1" do
  gem "activerecord", "~> 7.1.0"
  gem "railties", "~> 7.1.0"
end

lockfile "activerecord-7.2" do
  gem "activerecord", "~> 7.2.0"
  gem "rack", "~> 3.2.3"
  gem "railties", "~> 7.2.0"
end

lockfile do
  gem "activerecord", "~> 8.0.0"
  gem "rack", "~> 3.2", ">= 3.2.3"
  gem "railties", "~> 8.0.0"
end

group :development, :test do
  gem "debug", "~> 1.8"
  gem "pg", "~> 1.2"
  gem "rake", "~> 13.0"
  gem "rspec-mocks", "~> 3.5"
  gem "rspec-rails", "~> 8.0", ">= 8.0.0"
  gem "rubocop", "~> 1.10"
  gem "rubocop-inst",  "~> 1"
  gem "rubocop-rake",  "~> 0.5"
  gem "rubocop-rspec", "~> 3.0"
  gem "rubocop-rspec_rails", "~> 2.29"
  gem "simplecov", "~> 0.15"
end
