pipeline {
  agent {
    label "docker"
  }
  stages {
    stage('Build') {
      steps {
        ansiColor('xterm') {
          sh './build.sh'
        }
      }
    }
  }
}
