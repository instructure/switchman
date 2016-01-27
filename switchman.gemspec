$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "switchman/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "switchman"
  s.version     = Switchman::VERSION
  s.authors     = ["Cody Cutrer", "James Williams", "Jacob Fugal"]
  s.email       = ["cody@instructure.com"]
  s.homepage    = "http://www.instructure.com/"
  s.summary     = "Rails 4 sharding magic"
  s.description = "Sharding"
  s.license     = "MIT"

  s.files = Dir["{app,config,db,lib}/**/*"] + ["Rakefile"]
  s.test_files = Dir["spec/**/*"]

  s.required_ruby_version = '>= 2.0'

  s.add_dependency "railties", ">= 4.0", "< 4.3"
  s.add_dependency "activerecord", ">= 4.0", "< 4.3"
  s.add_dependency "shackles", ">= 1.0.10", "< 2.0"
  s.add_dependency 'open4', '1.3.0'

  s.add_development_dependency "mysql2", "~> 0.3"
  s.add_development_dependency "pg", "~> 0"
  s.add_development_dependency "rspec-rails", "~> 3.0"
  s.add_development_dependency "sqlite3", "~> 1.3"
end
