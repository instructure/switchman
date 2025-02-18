# frozen_string_literal: true

source "http://rubygems.org"

plugin "bundler-multilock", "1.3.4"
return unless Plugin.installed?("bundler-multilock")

Plugin.send(:load_plugin, "bundler-multilock")

gemspec

lockfile "activerecord-7.0" do
  gem "activerecord", "~> 7.0.0"
  gem "railties", "~> 7.0.0"
end

lockfile "activerecord-7.1" do
  gem "activerecord", "~> 7.1.0"
  gem "railties", "~> 7.1.0"
end

lockfile do
  gem "activerecord", "~> 7.2.0"
  gem "railties", "~> 7.2.0"
end
