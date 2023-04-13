#!/usr/bin/env rake
# frozen_string_literal: true

begin
  require 'bundler/setup'
rescue LoadError
  puts 'You must `gem install bundler` and `bundle install` to run rake tasks'
end
begin
  require 'rdoc/task'
rescue LoadError
  require 'rdoc/rdoc'
  require 'rake/rdoctask'
  RDoc::Task = Rake::RDocTask
end

RDoc::Task.new(:rdoc) do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title    = 'Switchman'
  rdoc.options << '--line-numbers'
  rdoc.rdoc_files.include('lib/**/*.rb')
end

load './spec/tasks/coverage.rake'
APP_RAKEFILE = File.expand_path('spec/dummy/Rakefile', __dir__)
load 'rails/tasks/engine.rake'

Bundler::GemHelper.install_tasks

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new

require 'rubocop/rake_task'

RuboCop::RakeTask.new do |task|
  task.options = ['-S']
end

task default: %i[spec]
