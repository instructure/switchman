FROM instructure/rvm

WORKDIR /app

USER root
RUN chown -R docker:docker /app
USER docker

# 2.7 already comes with 2.1.4; just make them the same
RUN /bin/bash -lc "rvm-exec 2.6 gem install bundler -v 2.1.4"

COPY --chown=docker:docker switchman.gemspec Gemfile /app/
COPY --chown=docker:docker lib/switchman/version.rb /app/lib/switchman/version.rb

RUN echo "gem: --no-document" >> ~/.gemrc
RUN mkdir -p .bundle coverage log \
             gemfiles/.bundle \
             spec/dummy/log \
             spec/dummy/tmp

RUN /bin/bash -lc "cd /app && rvm-exec 2.7 bundle install --jobs 5"
COPY --chown=docker:docker . /app
