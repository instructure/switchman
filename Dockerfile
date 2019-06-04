FROM instructure/rvm

WORKDIR /app

USER root
RUN chown -R docker:docker /app
USER docker

COPY --chown=docker:docker switchman.gemspec Gemfile /app/
COPY --chown=docker:docker lib/switchman/version.rb /app/lib/switchman/version.rb

RUN echo "gem: --no-document" >> ~/.gemrc
RUN mkdir -p .bundle coverage log \
             gemfiles/.bundle \
             spec/dummy/log \
             spec/dummy/tmp

RUN /bin/bash -lc "cd /app && rvm-exec 2.4 bundle install --jobs 5"
COPY --chown=docker:docker . /app
