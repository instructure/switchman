# frozen_string_literal: true

namespace :coverage do
  desc "Aggregate coverage across ruby/rails versions"
  task :report do
    require "simplecov"

    SimpleCov.collate Dir["coverage/*/coverage/.resultset.json"] do
      enable_coverage :branch
      # TODO: this branch coverage should probably be higher
      minimum_coverage line: 90, branch: 75
      # Ideally we'd bring both of these up a good bit too
      minimum_coverage_by_file line: 32, branch: 16
    end
  end
end
