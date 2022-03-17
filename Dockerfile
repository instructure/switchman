ARG  RUBY_VERSION=2.7
FROM ruby:${RUBY_VERSION}

WORKDIR /app

RUN /bin/bash -lc "gem install bundler -v 2.2.23"

ARG BUNDLE_GEMFILE
ENV BUNDLE_GEMFILE $BUNDLE_GEMFILE

RUN echo "gem: --no-document" >> ~/.gemrc

COPY . /app
RUN /bin/bash -lc "bundle install --jobs 5"
