$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "switchman/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "switchman"
  s.version     = Switchman::VERSION
  s.authors     = ["Cody Cutrer"]
  s.email       = ["cody@instructure.com"]
  s.homepage    = "http://www.instructure.com/"
  s.summary     = "Rails 3 sharding magic"
  s.description = "TODO: Description of Switchman."

  s.files = Dir["{app,config,db,lib}/**/*"] + ["Rakefile"]
  s.test_files = Dir["spec/**/*"]

  s.add_dependency "rails", "~> 3.2.13"
  s.add_dependency "shackles"

  s.add_development_dependency "debugger"
  s.add_development_dependency "pg"
  s.add_development_dependency "rspec-rails", "~> 2.0"
  s.add_development_dependency "sqlite3"
end
