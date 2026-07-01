// ══════════════════════════════════════════════════════════════════════════════
// Helper functions
// ══════════════════════════════════════════════════════════════════════════════

def generateEvent(Map args) {
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
    sh """
        set -eu
        echo "==> Sending ${args.eventType} [${args.status}] to Kafka via kcat container"

        # ── KEY FIX ───────────────────────────────────────────────────────
        # Compact the JSON to a single line first using python3, then pipe
        # it to kcat.  This guarantees kcat receives exactly ONE message
        # regardless of how many lines the pretty-printed JSON file has.
        #
        # Without this, kcat splits on newlines by default and each line
        # of the JSON becomes a separate Kafka message.
        # ──────────────────────────────────────────────────────────────────
        python3 -c "
import json, sys
with open('${args.fileName}') as f:
    print(json.dumps(json.load(f), separators=(',', ':')), end='')
" | docker exec -i kcat kcat \\
            -P \\
            -b kafka:29092 \\
            -t \${KAFKA_TOPIC} \\
            -k "\${SERVICE_NAME}:${args.eventType}" \\
            -H "content-type=application/json" \\
            -H "stage=${args.stage}" \\
            -H "status=${args.status}" \\
            -H "build_number=\${BUILD_NUMBER}"

        echo "==> ${args.eventType} [${args.status}] delivered as single Kafka message"
    """
}

def stageEvent(Map args) {
    args.status = 'SUCCESS'
    generateEvent(args)
    sendToKafka(args)
    archiveArtifacts artifacts: args.fileName, fingerprint: true
}

def stageEventFailure(Map args) {
    args.status = 'FAILURE'
    generateEvent(args)
    sendToKafka(args)
    archiveArtifacts artifacts: args.fileName, fingerprint: true
}

// ══════════════════════════════════════════════════════════════════════════════
// Pipeline
// ══════════════════════════════════════════════════════════════════════════════
pipeline {
    agent any

      triggers {
        pollSCM('H/1 * * * *')
    }

    environment {
        ANALYSIS_NAME = 'ccid-observabillity-flink-analysis'
        SERVICE_NAME  = 'bloodpressure-backend-service'
        KAFKA_TOPIC   = 'cicd-events'
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
        // ══════════════════════════════════════════════════════════════════
        stage('Aggregate Events JSON') {
            steps {
                script {
                    sh '''
                        set -eu
                        branch="${BRANCH_NAME:-${GIT_BRANCH:-main}}"
                        commit_sha="${GIT_COMMIT:-unknown}"
                        timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

                        read_status() {
                            python3 -c "import json; print(json.load(open('$1'))['event']['status'])" \
                                2>/dev/null || echo "UNKNOWN"
                        }

                        build_status=$(read_status build_started_event.json)
                        test_status=$(read_status test_started_event.json)
                        sonar_status=$(read_status sonarqube_started_event.json)
                        package_status=$(read_status package_started_event.json)

                        # Build the aggregated payload as pretty JSON first
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
                        echo "==> Aggregated events.json (pretty):"
                        cat events.json

                        # Compact to single line before sending to Kafka
                        echo "==> Sending PIPELINE_COMPLETED as single Kafka message"
                        python3 -c "
import json, sys
with open('events.json') as f:
    print(json.dumps(json.load(f), separators=(',', ':')), end='')
" | docker exec -i kcat kcat \
                            -P \
                            -b kafka:29092 \
                            -t "${KAFKA_TOPIC}" \
                            -k "${SERVICE_NAME}:PIPELINE_COMPLETED" \
                            -H "content-type=application/json" \
                            -H "stage=Aggregate" \
                            -H "status=SUCCESS" \
                            -H "build_number=${BUILD_NUMBER}"

                        echo "==> PIPELINE_COMPLETED delivered as single Kafka message"
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
                    echo "⚠️  Could not archive artefacts: ${e.message}"
                }
            }
        }
    }
}