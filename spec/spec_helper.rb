# frozen_string_literal: true

# This file is copied to spec/ when you run 'rails generate rspec:install'
ENV["RAILS_ENV"] ||= 'test'

require 'simplecov'
SimpleCov.start do
  add_filter 'db'
  add_filter 'lib/switchman/version.rb'
  add_filter 'lib/tasks'
  add_filter 'spec'
  track_files 'lib/**/*.rb'
end
SimpleCov.minimum_coverage(80)

require File.expand_path("../dummy/config/environment", __FILE__)
require 'byebug'
require 'rspec/rails'

require 'switchman/r_spec_helper'

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[Rails.root.join("../spec/support/**/*.rb")].each {|f| require f}

RSpec.configure do |config|
  config.infer_spec_type_from_file_location!
  config.raise_errors_for_deprecations!

  # If you're not using ActiveRecord, or you'd prefer not to run each of your
  # examples within a transaction, remove the following line or assign false
  # instead of true.
  config.use_transactional_fixtures = true

  # If true, the base class of anonymous controllers will be inferred
  # automatically. This will be the default behavior in future versions of
  # rspec-rails.
  config.infer_base_class_for_anonymous_controllers = false

  # Run specs in random order to surface order dependencies. If you find an
  # order dependency and want to debug it, you can fix the order by providing
  # the seed, which is printed after each run.
  #     --seed 1234
  config.order = "random"
end

def where_value(value)
  case value
  when ::Arel::Nodes::Casted
    value.value
  when ::Arel::Nodes::BindParam
    where_value(value.value)
  when ::ActiveRecord::Relation::QueryAttribute
    value.value_before_type_cast
  when Array
    value.map{|v| where_value(v)}
  else
    value
  end
end

def predicates(relation)
  relation.where_clause.send(:predicates)
end

def bind_values(relation)
  predicates(relation).map{|p| where_value(p.right)}.flatten
end
