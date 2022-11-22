#!/bin/bash
# shellcheck shell=bash

set -e

current_version=$(ruby -e "require '$(pwd)/lib/switchman/version.rb'; puts Switchman::VERSION;")
existing_versions=$(gem list --exact switchman --remote --all | grep -o '\((.*)\)$' | tr -d '() ')

if [[ $existing_versions == *$current_version* ]]; then
  echo "Gem has already been published ... skipping ..."
else
  gem build ./switchman.gemspec
  find switchman-*.gem | xargs gem push
fi
