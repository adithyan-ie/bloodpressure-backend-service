pipeline {
    agent any

    environment {
        ANALYSIS_NAME = 'ccid-observabillity-flink-analysis'
        SERVICE_NAME = 'bloodpressure-backend-service'
    }

    stages {
        stage('Build Started') {
            steps {
                echo "Build started for ${env.SERVICE_NAME}"
            }
        }

        stage('Test Started') {
            steps {
                echo "Test started for ${env.SERVICE_NAME}"
            }
        }

        stage('SonarQube Started') {
            steps {
                echo "SonarQube scan started for ${env.SERVICE_NAME}"
            }
        }

        stage('Package Started') {
            steps {
                echo "Package started for ${env.SERVICE_NAME}"
            }
        }

        stage('Generate Events JSON') {
            steps {
                sh '''
                    set -eu

                    branch="${BRANCH_NAME:-${GIT_BRANCH:-main}}"
                    commit_sha="${GIT_COMMIT:-}"
                    timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

                    cat > events.json <<EOF
{
  "analysis_name": "${ANALYSIS_NAME}",
  "analysis_type": "flink",
  "service_name": "${SERVICE_NAME}",
  "job_name": "${JOB_NAME}",
  "build_number": "${BUILD_NUMBER}",
  "build_url": "${BUILD_URL:-}",
  "events": [
    {
      "event_id": "${JOB_NAME}-${BUILD_NUMBER}-1",
      "pipeline_id": "${JOB_NAME}",
      "repository_id": "${SERVICE_NAME}",
      "analysis_name": "${ANALYSIS_NAME}",
      "analysis_type": "flink",
      "service_name": "${SERVICE_NAME}",
      "branch": "${branch}",
      "commit_sha": "${commit_sha}",
      "stage": "Build Started",
      "event_type": "BUILD_STARTED",
      "event_timestamp": "${timestamp}",
      "status": "SUCCESS"
    },
    {
      "event_id": "${JOB_NAME}-${BUILD_NUMBER}-2",
      "pipeline_id": "${JOB_NAME}",
      "repository_id": "${SERVICE_NAME}",
      "analysis_name": "${ANALYSIS_NAME}",
      "analysis_type": "flink",
      "service_name": "${SERVICE_NAME}",
      "branch": "${branch}",
      "commit_sha": "${commit_sha}",
      "stage": "Test Started",
      "event_type": "TEST_STARTED",
      "event_timestamp": "${timestamp}",
      "status": "SUCCESS"
    },
    {
      "event_id": "${JOB_NAME}-${BUILD_NUMBER}-3",
      "pipeline_id": "${JOB_NAME}",
      "repository_id": "${SERVICE_NAME}",
      "analysis_name": "${ANALYSIS_NAME}",
      "analysis_type": "flink",
      "service_name": "${SERVICE_NAME}",
      "branch": "${branch}",
      "commit_sha": "${commit_sha}",
      "stage": "SonarQube Started",
      "event_type": "SONARQUBE_STARTED",
      "event_timestamp": "${timestamp}",
      "status": "SUCCESS"
    },
    {
      "event_id": "${JOB_NAME}-${BUILD_NUMBER}-4",
      "pipeline_id": "${JOB_NAME}",
      "repository_id": "${SERVICE_NAME}",
      "analysis_name": "${ANALYSIS_NAME}",
      "analysis_type": "flink",
      "service_name": "${SERVICE_NAME}",
      "branch": "${branch}",
      "commit_sha": "${commit_sha}",
      "stage": "Package Started",
      "event_type": "PACKAGE_STARTED",
      "event_timestamp": "${timestamp}",
      "status": "SUCCESS"
    }
  ]
}
EOF

                    echo "Generated Jenkins pipeline events JSON:"
                    cat events.json
                '''
                archiveArtifacts artifacts: 'events.json', fingerprint: true
            }
        }
    }

    post {
        success {
            echo "Pipeline completed successfully for ${env.SERVICE_NAME}"
        }
        failure {
            echo "Pipeline failed for ${env.SERVICE_NAME}"
        }
    }
}
