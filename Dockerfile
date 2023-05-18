ARG  RUBY_VERSION=2.7
FROM ruby:${RUBY_VERSION}

RUN apt-get update && \
	apt-get install -y --no-install-recommends postgresql-client && \
	rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN /bin/bash -lc "gem install bundler -v 2.2.23"

ARG BUNDLE_GEMFILE
ENV BUNDLE_GEMFILE $BUNDLE_GEMFILE

RUN echo "gem: --no-document" >> ~/.gemrc

COPY . /app
RUN /bin/bash -lc "bundle install --jobs 5"
