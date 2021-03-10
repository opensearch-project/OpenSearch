pipeline {
    agent { label 'search-cloud-ec2-c518xlarge' }



    stages {

        stage('DCO Check') {
            when { branch pattern: "PR-\\d+", comparator: "REGEXP"}
            steps {
                sh (
                  script: './dev-tools/signoff-check.sh remotes/origin/' + CHANGE_TARGET + ' ' + GIT_COMMIT,
                )
            }
        }
        stage('Build') {
            steps {
                echo 'Building..'
                // Disable backward compability tasks
                sh './gradlew check --no-daemon --no-scan -Pbwc_tests_enabled=false'
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying..'
            }
        }
    }
}
