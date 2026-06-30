// ══════════════════════════════════════════════════════════════════════════════
// Shared library functions — defined once, called from every stage.
//
//  generateEvent(args)   → writes a per-stage JSON file and returns the path
//  sendToKafka(args)     → sends a JSON file to Kafka via kcat
//  stageEvent(args)      → generates + sends in one call (normal happy path)
//  stageEventFailure(args) → same but overwrites status to FAILURE
// ══════════════════════════════════════════════════════════════════════════════

def generateEvent(Map args) {
    /*
     * Writes one JSON event file for a pipeline stage.
     *
     * Required args:
     *   eventType   String  e.g. "BUILD_STARTED"
     *   stage       String  e.g. "Build Started"
     *   status      String  "SUCCESS" | "FAILURE"
     *   fileName    String  output file name, e.g. "build_started_event.json"
     *
     * Returns: the file name that was written (same as args.fileName)
     */
    sh """
        set -eu
        branch="\${BRANCH_NAME:-\${GIT_BRANCH:-main}}"
        commit_sha="\${GIT_COMMIT:-unknown}"
        timestamp="\$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        event_id="\${JOB_NAME}-\${BUILD_NUMBER}-${args.eventType.toLowerCase().replace('_', '-')}"

        cat > ${args.fileName} <<EOF
{
  "analysis_name":   "\${ANALYSIS_NAME}",
  "analysis_type":   "flink",
  "service_name":    "\${SERVICE_NAME}",
  "job_name":        "\${JOB_NAME}",
  "build_number":    "\${BUILD_NUMBER}",
  "build_url":       "\${BUILD_URL:-}",
  "event": {
    "event_id":        "\${event_id}",
    "pipeline_id":     "\${JOB_NAME}",
    "repository_id":   "\${SERVICE_NAME}",
    "analysis_name":   "\${ANALYSIS_NAME}",
    "analysis_type":   "flink",
    "service_name":    "\${SERVICE_NAME}",
    "branch":          "\${branch}",
    "commit_sha":      "\${commit_sha}",
    "stage":           "${args.stage}",
    "event_type":      "${args.eventType}",
    "event_timestamp": "\${timestamp}",
    "status":          "${args.status}"
  }
}
EOF
        echo "==> Generated ${args.fileName}"
        cat ${args.fileName}
    """
    return args.fileName
}

def sendToKafka(Map args) {
    /*
     * Publishes a JSON file to the configured Kafka topic via kcat.
     *
     * Required args:
     *   fileName    String  path to the JSON file to send
     *   eventType   String  used as the message key suffix
     *   stage       String  used as a Kafka message header
     *   status      String  "SUCCESS" | "FAILURE" — added as a header
     */
    sh """
        set -eu
        echo "==> Sending ${args.eventType} [${args.status}] to Kafka topic: \${KAFKA_TOPIC}"
        cat ${args.fileName} | kcat \\
            -P \\
            -b "\${KAFKA_BOOTSTRAP}" \\
            -t "\${KAFKA_TOPIC}" \\
            -k "\${SERVICE_NAME}:${args.eventType}" \\
            -H "content-type=application/json" \\
            -H "stage=${args.stage}" \\
            -H "status=${args.status}" \\
            -H "build_number=\${BUILD_NUMBER}"
        echo "==> ${args.eventType} [${args.status}] delivered to Kafka"
    """
}

def stageEvent(Map args) {
    /*
     * Convenience wrapper: generate JSON + send to Kafka for a SUCCESS event.
     * Also archives the file as a Jenkins build artefact.
     *
     * Required args:  eventType, stage, fileName
     * Status is always "SUCCESS" — use stageEventFailure for failures.
     */
    args.status = 'SUCCESS'
    generateEvent(args)
    sendToKafka(args)
    archiveArtifacts artifacts: args.fileName, fingerprint: true
}

def stageEventFailure(Map args) {
    /*
     * Convenience wrapper: generate JSON + send to Kafka for a FAILURE event.
     * Intended for use inside post { failure } blocks.
     *
     * Required args:  eventType, stage, fileName
     * Status is always "FAILURE".
     */
    args.status = 'FAILURE'
    generateEvent(args)
    sendToKafka(args)
    archiveArtifacts artifacts: args.fileName, fingerprint: true
}

// ══════════════════════════════════════════════════════════════════════════════
// Pipeline definition
// ══════════════════════════════════════════════════════════════════════════════
pipeline {
    agent any
    environment {
        ANALYSIS_NAME   = 'ccid-observabillity-flink-analysis'
        SERVICE_NAME    = 'bloodpressure-backend-service'

        // Kafka — value stored in Jenkins Credentials Store as Secret Text
        KAFKA_BOOTSTRAP = 'localhost:9092'  // e.g. localhost:9092
        KAFKA_TOPIC     = 'cicd-events'
    }

    stages {

        // ══════════════════════════════════════════════════════════════════
        // STAGE 1 — Build Started
        // ══════════════════════════════════════════════════════════════════
        stage('Build Started') {
            steps {
                echo "Build started for ${env.SERVICE_NAME}"
                script {
                    stageEvent(
                        eventType : 'BUILD_STARTED',
                        stage     : 'Build Started',
                        fileName  : 'build_started_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        stageEventFailure(
                            eventType : 'BUILD_STARTED',
                            stage     : 'Build Started',
                            fileName  : 'build_started_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 2 — Test Started
        // ══════════════════════════════════════════════════════════════════
        stage('Test Started') {
            steps {
                echo "Test started for ${env.SERVICE_NAME}"
                script {
                    stageEvent(
                        eventType : 'TEST_STARTED',
                        stage     : 'Test Started',
                        fileName  : 'test_started_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        stageEventFailure(
                            eventType : 'TEST_STARTED',
                            stage     : 'Test Started',
                            fileName  : 'test_started_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 3 — SonarQube Started
        // ══════════════════════════════════════════════════════════════════
        stage('SonarQube Started') {
            steps {
                echo "SonarQube scan started for ${env.SERVICE_NAME}"
                script {
                    stageEvent(
                        eventType : 'SONARQUBE_STARTED',
                        stage     : 'SonarQube Started',
                        fileName  : 'sonarqube_started_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        stageEventFailure(
                            eventType : 'SONARQUBE_STARTED',
                            stage     : 'SonarQube Started',
                            fileName  : 'sonarqube_started_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 4 — Package Started
        // ══════════════════════════════════════════════════════════════════
        stage('Package Started') {
            steps {
                echo "Package started for ${env.SERVICE_NAME}"
                script {
                    stageEvent(
                        eventType : 'PACKAGE_STARTED',
                        stage     : 'Package Started',
                        fileName  : 'package_started_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        stageEventFailure(
                            eventType : 'PACKAGE_STARTED',
                            stage     : 'Package Started',
                            fileName  : 'package_started_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 5 — Aggregate Events JSON
        // Merges all per-stage files into one events.json and sends a
        // single PIPELINE_COMPLETED summary event to Kafka.
        // ══════════════════════════════════════════════════════════════════
        stage('Aggregate Events JSON') {
            steps {
                script {
                    sh '''
                        set -eu
                        branch="${BRANCH_NAME:-${GIT_BRANCH:-main}}"
                        commit_sha="${GIT_COMMIT:-unknown}"
                        timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

                        # Read real statuses from each per-stage file
                        read_status() {
                            python3 -c "import json; print(json.load(open('$1'))['event']['status'])" \
                                2>/dev/null || echo "UNKNOWN"
                        }

                        build_status=$(read_status build_started_event.json)
                        test_status=$(read_status test_started_event.json)
                        sonar_status=$(read_status sonarqube_started_event.json)
                        package_status=$(read_status package_started_event.json)

                        cat > events.json <<EOF
{
  "analysis_name":  "${ANALYSIS_NAME}",
  "analysis_type":  "flink",
  "service_name":   "${SERVICE_NAME}",
  "job_name":       "${JOB_NAME}",
  "build_number":   "${BUILD_NUMBER}",
  "build_url":      "${BUILD_URL:-}",
  "aggregated_at":  "${timestamp}",
  "events": [
    {
      "event_id":        "${JOB_NAME}-${BUILD_NUMBER}-build-started",
      "pipeline_id":     "${JOB_NAME}",
      "repository_id":   "${SERVICE_NAME}",
      "analysis_name":   "${ANALYSIS_NAME}",
      "analysis_type":   "flink",
      "service_name":    "${SERVICE_NAME}",
      "branch":          "${branch}",
      "commit_sha":      "${commit_sha}",
      "stage":           "Build Started",
      "event_type":      "BUILD_STARTED",
      "event_timestamp": "${timestamp}",
      "status":          "${build_status}"
    },
    {
      "event_id":        "${JOB_NAME}-${BUILD_NUMBER}-test-started",
      "pipeline_id":     "${JOB_NAME}",
      "repository_id":   "${SERVICE_NAME}",
      "analysis_name":   "${ANALYSIS_NAME}",
      "analysis_type":   "flink",
      "service_name":    "${SERVICE_NAME}",
      "branch":          "${branch}",
      "commit_sha":      "${commit_sha}",
      "stage":           "Test Started",
      "event_type":      "TEST_STARTED",
      "event_timestamp": "${timestamp}",
      "status":          "${test_status}"
    },
    {
      "event_id":        "${JOB_NAME}-${BUILD_NUMBER}-sonarqube-started",
      "pipeline_id":     "${JOB_NAME}",
      "repository_id":   "${SERVICE_NAME}",
      "analysis_name":   "${ANALYSIS_NAME}",
      "analysis_type":   "flink",
      "service_name":    "${SERVICE_NAME}",
      "branch":          "${branch}",
      "commit_sha":      "${commit_sha}",
      "stage":           "SonarQube Started",
      "event_type":      "SONARQUBE_STARTED",
      "event_timestamp": "${timestamp}",
      "status":          "${sonar_status}"
    },
    {
      "event_id":        "${JOB_NAME}-${BUILD_NUMBER}-package-started",
      "pipeline_id":     "${JOB_NAME}",
      "repository_id":   "${SERVICE_NAME}",
      "analysis_name":   "${ANALYSIS_NAME}",
      "analysis_type":   "flink",
      "service_name":    "${SERVICE_NAME}",
      "branch":          "${branch}",
      "commit_sha":      "${commit_sha}",
      "stage":           "Package Started",
      "event_type":      "PACKAGE_STARTED",
      "event_timestamp": "${timestamp}",
      "status":          "${package_status}"
    }
  ]
}
EOF
                        echo "==> Aggregated events.json"
                        cat events.json

                        echo "==> Sending PIPELINE_COMPLETED to Kafka topic: ${KAFKA_TOPIC}"
                        cat events.json | kcat \
                            -P \
                            -b "${KAFKA_BOOTSTRAP}" \
                            -t "${KAFKA_TOPIC}" \
                            -k "${SERVICE_NAME}:PIPELINE_COMPLETED" \
                            -H "content-type=application/json" \
                            -H "stage=Aggregate" \
                            -H "status=SUCCESS" \
                            -H "build_number=${BUILD_NUMBER}"
                        echo "==> PIPELINE_COMPLETED delivered to Kafka"
                    '''
                    archiveArtifacts artifacts: 'events.json', fingerprint: true
                }
            }
        }
    }

    post {
        success {
            echo "✅ Pipeline completed successfully for ${env.SERVICE_NAME}"
        }
        failure {
            echo "❌ Pipeline failed for ${env.SERVICE_NAME}"
        }
       always {
    script {
        try {
            archiveArtifacts artifacts: '*_event.json', allowEmptyArchive: true
        } catch (Exception e) {
            echo "⚠️  Could not archive — no workspace: ${e.message}"
        }
    }
        }
    }
}
