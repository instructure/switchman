#!/bin/bash -ex

function cleanup() {
  exit_code=$?
  set +e
  docker cp $(docker ps --latest --quiet):/app/coverage .
  docker-compose kill
  docker-compose rm -f
  exit $exit_code
}
trap cleanup INT TERM EXIT

docker-compose pull postgres
docker-compose up -d postgres
docker-compose build --pull app

function test_ruby_version() {
  docker-compose run --rm app /bin/bash -lc \
    "rvm-exec $1 bundle install --jobs 5"
  docker-compose run --rm app /bin/bash -lc \
    "rvm-exec $1 bundle exec appraisal install --jobs 5"
  docker-compose run --rm app /bin/bash -lc \
    "rvm-exec $1 bundle exec rake db:drop db:create db:migrate"
  docker-compose run app /bin/bash -lc \
    "rvm-exec $1 bundle exec appraisal rspec --format doc"
  docker cp $(docker ps --latest --quiet):/app/coverage .
}

test_ruby_version "2.3"
test_ruby_version "2.4"
