source "http://rubygems.org"

# Declare your gem's dependencies in switchman.gemspec.
# Bundler will treat runtime dependencies like base dependencies, and
# development dependencies will be added by default to the :development group.
gemspec

# if put in the gemspec, the :require => false is ignored
gem 'mocha', :require => false

group :development do
  gem 'appraisal'
  gem 'debugger', platform: :mri_19
  gem 'byebug', platform: [:mri_20, :mri_21, :mri_22]
end
