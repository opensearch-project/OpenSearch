pipeline {
    agent { label 'search-cloud-ec2-c518xlarge' }


    stages {
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
