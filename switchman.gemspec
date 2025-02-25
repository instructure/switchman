# frozen_string_literal: true

# Maintain your gem's version:
require_relative "lib/switchman/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "switchman"
  s.version     = Switchman::VERSION
  s.authors     = ["Cody Cutrer", "James Williams", "Jacob Fugal"]
  s.email       = ["cody@instructure.com"]
  s.homepage    = "http://www.instructure.com/"
  s.summary     = "Rails sharding magic"
  s.description = "Sharding"
  s.license     = "MIT"

  s.metadata = {
    "rubygems_mfa_required" => "true",
    "source_code_uri" => "https://github.com/instructure/switchman"
  }

  s.files = Dir["{app,db,lib}/**/*", "Rakefile"]

  s.required_ruby_version = ">= 3.1"

  s.add_dependency "activerecord", ">= 7.0", "< 7.2"
  s.add_dependency "guardrail", "~> 3.0.1"
  s.add_dependency "parallel", "~> 1.22"
  s.add_dependency "railties", ">= 7.0", "< 7.2"

  s.add_development_dependency "debug", "~> 1.8"
  s.add_development_dependency "pg", "~> 1.2"
  s.add_development_dependency "rake", "~> 13.0"
  s.add_development_dependency "rspec-mocks", "~> 3.5"
  s.add_development_dependency "rspec-rails", "~> 6.0"
  s.add_development_dependency "rubocop", "~> 1.10"
  s.add_development_dependency "rubocop-inst",  "~> 1"
  s.add_development_dependency "rubocop-rake",  "~> 0.5"
  s.add_development_dependency "rubocop-rspec", "~> 3.0"
  s.add_development_dependency "rubocop-rspec_rails", "~> 2.29"
  s.add_development_dependency "simplecov", "~> 0.15"
end
