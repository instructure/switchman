# Switchman Development

A simple docker environment has been provided for spinning up and testing this
gem with multiple versions of Ruby. This requires docker and docker-compose to
be installed. To get started, run the following:

```bash
./build.sh
```

This will install the gem in a docker image with all versions of Ruby installed,
and install all gem dependencies in the Ruby 2.4 set of gems. It will also
download and spin up a PostgreSQL container for use with specs.

The first build will take a long time, however, docker images and gems are
cached, making additional runs significantly faster.

Individual spec runs can be started by resetting the DB, and running RSpec with
your custom options like so:

```bash
docker-compose run --rm app /bin/bash -lc \
  "rvm-exec 2.4 bundle exec rake db:drop db:create db:migrate"
docker-compose run --rm app /bin/bash -lc \
  "rvm-exec 2.4 bundle exec rspec spec/lib/rails_spec.rb"
```

If you'd like to mount your git checkout within the docker container running
tests so changes are easier to test, use the override provided:

```bash
cp docker-compose.override.example.yml docker-compose.override.yml
```

## Linux Tips

When running on Linux, everything should work out of the box, but if you enable
the docker-compose override example config (to mount your git checkout into the
container), you will want to pre-create all lock files, and give world write
permissions so the containers can update them, since they're not located in any
regular docker volumes:

```bash
touch Gemfile.lock \
      gemfiles/activerecord_5.0.gemfile.lock \
      gemfiles/activerecord_5.1.gemfile.lock
chmod a+w Gemfile.lock gemfiles/*.lock
```

## Code Coverage

Code coverage reports are enabled by default, and when using build.sh, the data
will automatically be copied out of the docker containers running specs, and
into your local "coverage" directory.

## Making a new Release

To install this gem onto your local machine, run `bundle exec rake install`. To
release a new version, update the version number in `version.rb`, and then just
run `bundle exec rake release`, which will create a git tag for the version,
push git commits and tags, and push the `.gem` file to
[rubygems.org](https://rubygems.org).