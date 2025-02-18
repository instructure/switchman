# frozen_string_literal: true

require "logger" # Fix concurrent-ruby removing logger dependency which Rails 7.0 itself does not have
require "rubygems"
gemfile = File.expand_path("../../../Gemfile", __dir__)

if File.exist?(ENV["BUNDLE_GEMFILE"] || gemfile)
  ENV["BUNDLE_GEMFILE"] ||= gemfile
  require "bundler"
  Bundler.setup
end

$:.unshift File.expand_path("../../../lib", __dir__)
