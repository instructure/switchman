#! /usr/bin/env groovy

def matrix_stages = []

pipeline {
  agent { label 'docker' }

  environment {
    // Make sure we're ignoring any override files that may be present
    COMPOSE_FILE = "docker-compose.yml"
  }

  stages {
    stage('Test') {
      matrix {
        agent { label 'docker' }
        axes {
          axis {
            name 'RUBY_VERSION'
            values '3.2', '3.3', '3.4'
          }
          axis {
            name 'LOCKFILE'
            values 'activerecord-7.1', 'activerecord-7.2', 'Gemfile.lock'
          }
        }
        stages {
          stage('Build') {
            steps {
              script {
                matrix_stages.add("switchman_rspec_ruby_${RUBY_VERSION}_${LOCKFILE}")
              }
              sh "rm -rf coverage"
              // Allow postgres to initialize while the build runs
              sh 'docker-compose up -d postgres'
              sh "docker-compose build --pull --build-arg RUBY_VERSION=${RUBY_VERSION} app"
              sh "BUNDLE_LOCKFILE=${LOCKFILE} docker-compose run --rm app bundle exec rake db:drop db:create db:migrate"
              sh "BUNDLE_LOCKFILE=${LOCKFILE} docker-compose run --name switchman_rspec_runner app bundle exec rake"
              sh "docker cp switchman_rspec_runner:/app/coverage coverage"
              sh "docker rm switchman_rspec_runner"
              stash name: "switchman_rspec_ruby_${RUBY_VERSION}_${LOCKFILE}_coverage", includes: "coverage/**"
            }
          }
        }

        post {
          cleanup {
            sh 'docker-compose down --remove-orphans --rmi all'
          }
        }
      }
    }

    stage('Coverage Report') {
      steps {
        script {
          sh "rm -rf coverage"
          matrix_stages.each {
            sh "mkdir -p coverage/${it}"
            dir("coverage/${it}") {
              unstash("${it}_coverage")
            }
          }
        }
        sh "docker-compose build"
        sh "docker-compose run --name switchman_coverage_reporter app bundle exec rake coverage:report"
        sh "docker cp switchman_coverage_reporter:/app/coverage coverage"
        sh "docker rm switchman_coverage_reporter"
        publishHTML target: [
          reportName: "Code Coverage",
          reportDir: "coverage",
          reportFiles: "coverage/index.html",
          keepAll: true
        ]
      }
    }

    stage('Lint') {
      steps {
        sh """
        # Always rebuild the image so that we don't accidentally reuse one that had a custom RUBY_VERSION / BUNDLE_GEMFILE
        docker-compose build --pull
        docker-compose run --rm app bin/rubocop
        """
      }
    }

    stage('Deploy') {
      when {
        allOf {
          expression { GERRIT_BRANCH == "master" }
          environment name: "GERRIT_EVENT_TYPE", value: "change-merged"
        }
      }
      steps {
        lock( // only one build enters the lock
          resource: "${env.JOB_NAME}" // use the job name as lock resource to make the mutual exclusion only for builds from the same branch/tag
        ) {
          withCredentials([string(credentialsId: 'rubygems-rw', variable: 'GEM_HOST_API_KEY')]) {
            sh 'docker-compose run -e GEM_HOST_API_KEY --rm app /bin/bash -lc "./bin/publish.sh"'
          }
        }
      }
    }
  }

  post {
    cleanup {
      sh 'docker-compose down --remove-orphans --rmi all'
    }
  }
}
