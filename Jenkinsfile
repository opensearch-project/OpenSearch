pipeline {
    agent search-cloud-ec2-c518xlarge

    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                sh './gradlew check --no-daemon'
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
