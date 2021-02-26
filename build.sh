#!/bin/bash
set -ex

function cleanup() {
  exit_code=$?
  set +e
  docker cp $(docker ps --latest --quiet):/app/coverage .
  docker-compose kill
  docker-compose rm -f
  exit $exit_code
}
trap cleanup INT TERM EXIT

cp spec/dummy/config/database.yml.docker spec/dummy/config/database.yml
docker-compose pull postgres
docker-compose up -d postgres
docker-compose build --pull app

function test_ruby_version() {
  # test_ruby_version ruby_version appraisals
  # or just test_ruby_version ruby_version for all
  ruby_version=$1
  shift
  docker-compose run --rm app /bin/bash -lc \
    "rvm-exec $ruby_version bundle install --jobs 5"
  docker-compose run --rm app /bin/bash -lc \
    "rvm-exec $ruby_version bundle exec rake db:drop db:create db:migrate"
  if [ $# == 0 ] ; then
    docker-compose run --rm app /bin/bash -lc \
      "rvm-exec $ruby_version bundle exec appraisal bundle install --jobs 5"
    docker-compose run app /bin/bash -lc \
        "rvm-exec $ruby_version bundle exec appraisal rspec --format doc"
    docker-compose run app /bin/bash -lc \
        "rvm-exec $ruby_version bundle exec appraisal rubocop"
  else
    for appraisal_version in $* ; do
      docker-compose run --rm app /bin/bash -lc \
        "rvm-exec $ruby_version bundle exec appraisal $appraisal_version bundle install --jobs 5"
      docker-compose run app /bin/bash -lc \
        "rvm-exec $ruby_version bundle exec appraisal $appraisal_version rspec --format doc"
      docker-compose run app /bin/bash -lc \
        "rvm-exec $ruby_version bundle exec appraisal $appraisal_version rubocop"
    done
  fi
  docker cp $(docker ps --latest --quiet):/app/coverage .
}

test_ruby_version 2.6
test_ruby_version 2.7
