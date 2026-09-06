// ══════════════════════════════════════════════════════════════════════════════
// Helper functions
// ══════════════════════════════════════════════════════════════════════════════

def generateEvent(Map args) {
    sh """
        set -eu
        branch="\${BRANCH_NAME:-\${GIT_BRANCH:-main}}"
        commit_sha="\${GIT_COMMIT:-unknown}"
        timestamp="\$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        event_id="\${JOB_NAME}-\${BUILD_NUMBER}-${args.eventType.toLowerCase().replace('_', '-')}${args.idSuffix ? '-' + args.idSuffix : ''}"

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

// The kcat container is the single source of truth for where Kafka is —
// its KCAT_BROKERS env var is set correctly for whatever topology it was
// started with (same-host docker-compose.yml here uses "kafka:29092"; the
// reference Flink job repo's deploy/AWS/jenkins/docker-compose.yml sets it
// to "${DATA_LAYER_HOST}:9092" for a Kafka running on a separate EC2). This
// pipeline never needs to know or guess that address itself — it just asks
// kcat, exactly like the reference repo's own documented usage
// (`kcat -P -b $KCAT_BROKERS ...`). Resolved once, in the 'Resolve Kafka
// Broker' stage, and reused via env.KAFKA_BROKER for the rest of the build
// rather than re-querying kcat on every single event.
def kafkaBroker() {
    return sh(
        script: 'docker exec kcat sh -c \'echo "$KCAT_BROKERS"\'',
        returnStdout: true
    ).trim()
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
            -b "\${KAFKA_BROKER}" \\
            -t \${KAFKA_TOPIC} \\
            -p 0 \\
            -k "\${SERVICE_NAME}:${args.eventType}" \\
            -H "content-type=application/json" \\
            -H "stage=${args.stage}" \\
            -H "status=${args.status}" \\
            -H "build_number=\${BUILD_NUMBER}"

        echo "==> ${args.eventType} [${args.status}] produced to Kafka"
    """
}

// Kafka is an observability side-channel, not the pipeline's actual work —
// a broker hiccup must never be what fails the build. Any failure in
// generate/send is caught here and only ever softens the result to
// UNSTABLE, and only when the build isn't already FAILED for a real reason
// (e.g. this same call reporting a genuine stage failure from a
// post{failure{}} block while Kafka also happens to be down) — a Kafka
// hiccup must never downgrade an actual pipeline failure back to UNSTABLE.
def emitStageEvent(Map args) {
    try {
        generateEvent(args)
        sendToKafka(args)
        archiveArtifacts artifacts: args.fileName, fingerprint: true
    } catch (Exception e) {
        echo "⚠️  Kafka event emission failed for ${args.eventType} [${args.stage}]: ${e.message}"
        if (currentBuild.result != 'FAILURE') {
            currentBuild.result = 'UNSTABLE'
        }
    }
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
        // No Kafka host/port here on purpose — the kcat container (started
        // separately, e.g. via the reference Flink job repo's
        // deploy/AWS/jenkins/docker-compose.yml) already knows its own
        // broker address via its KCAT_BROKERS env var. See kafkaBroker().
        // KAFKA_BROKER itself is resolved once, in the 'Resolve Kafka
        // Broker' stage below, and reused by every later stage.
    }

    stages {

        // ══════════════════════════════════════════════════════════════════
        // STAGE 0 — Resolve Kafka Broker
        // Queries the kcat container's own KCAT_BROKERS env var exactly
        // once per build and stores it in env.KAFKA_BROKER for every later
        // stage to reuse, instead of re-querying kcat on every single event.
        // A failure here is a Kafka-side problem, not a pipeline failure —
        // same UNSTABLE-not-FAILURE handling as emitStageEvent, since every
        // later Kafka push will independently hit (and swallow) the same
        // failure anyway if the broker can't be resolved.
        // ══════════════════════════════════════════════════════════════════
        stage('Resolve Kafka Broker') {
            steps {
                script {
                    try {
                        env.KAFKA_BROKER = kafkaBroker()
                        echo "==> Kafka broker resolved from kcat container: ${env.KAFKA_BROKER}"
                    } catch (Exception e) {
                        echo "⚠️  Could not resolve Kafka broker from kcat container: ${e.message}"
                        if (currentBuild.result != 'FAILURE') {
                            currentBuild.result = 'UNSTABLE'
                        }
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 1 — Build
        // Scripted fail-then-retry-succeeds: BUILD_FAILED followed later by
        // BUILD_SUCCESS for the same pipeline_id (JOB_NAME) is exactly what
        // DoraOperators.MttrProcessFn keys off, so this is what actually
        // produces a Mean Time to Recovery data point in the Flink job.
        // ══════════════════════════════════════════════════════════════════
        stage('Build') {
            steps {
                script {
                    emitStageEvent(
                        eventType : 'BUILD_STARTED',
                        stage     : 'Build',
                        status    : 'SUCCESS',
                        fileName  : 'build_started_event.json'
                    )
                    echo "Simulating build attempt #1 for ${env.SERVICE_NAME} (mock pipeline — scripted to fail)"
                    emitStageEvent(
                        eventType : 'BUILD_FAILED',
                        stage     : 'Build',
                        status    : 'FAILURE',
                        fileName  : 'build_failed_event.json'
                    )
                    echo "Retrying build for ${env.SERVICE_NAME}"
                    emitStageEvent(
                        eventType : 'BUILD_STARTED',
                        stage     : 'Build',
                        status    : 'SUCCESS',
                        fileName  : 'build_retry_started_event.json',
                        idSuffix  : 'retry'
                    )
                    echo "Simulating build attempt #2 for ${env.SERVICE_NAME} (succeeds, closing the MTTR window)"
                    emitStageEvent(
                        eventType : 'BUILD_SUCCESS',
                        stage     : 'Build',
                        status    : 'SUCCESS',
                        fileName  : 'build_success_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        emitStageEvent(
                            eventType : 'BUILD_FAILED',
                            stage     : 'Build',
                            status    : 'FAILURE',
                            fileName  : 'build_failed_final_event.json',
                            idSuffix  : 'final'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 2 — Test
        // ══════════════════════════════════════════════════════════════════
        stage('Test') {
            steps {
                script {
                    emitStageEvent(
                        eventType : 'TEST_STARTED',
                        stage     : 'Test',
                        status    : 'SUCCESS',
                        fileName  : 'test_started_event.json'
                    )
                    echo "Simulating tests for ${env.SERVICE_NAME} (mock pipeline — no real tests run)"
                    emitStageEvent(
                        eventType : 'TEST_SUCCESS',
                        stage     : 'Test',
                        status    : 'SUCCESS',
                        fileName  : 'test_success_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        emitStageEvent(
                            eventType : 'TEST_FAILED',
                            stage     : 'Test',
                            status    : 'FAILURE',
                            fileName  : 'test_failed_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 3 — SonarQube
        // ══════════════════════════════════════════════════════════════════
        stage('SonarQube') {
            steps {
                script {
                    emitStageEvent(
                        eventType : 'SONARQUBE_STARTED',
                        stage     : 'SonarQube',
                        status    : 'SUCCESS',
                        fileName  : 'sonarqube_started_event.json'
                    )
                    echo "Simulating SonarQube scan for ${env.SERVICE_NAME} (mock pipeline — no real scan runs)"
                    emitStageEvent(
                        eventType : 'SONARQUBE_SUCCESS',
                        stage     : 'SonarQube',
                        status    : 'SUCCESS',
                        fileName  : 'sonarqube_success_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        emitStageEvent(
                            eventType : 'SONARQUBE_FAILED',
                            stage     : 'SonarQube',
                            status    : 'FAILURE',
                            fileName  : 'sonarqube_failed_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 4 — Package
        // ══════════════════════════════════════════════════════════════════
        stage('Package') {
            steps {
                script {
                    emitStageEvent(
                        eventType : 'PACKAGE_STARTED',
                        stage     : 'Package',
                        status    : 'SUCCESS',
                        fileName  : 'package_started_event.json'
                    )
                    echo "Simulating packaging for ${env.SERVICE_NAME} (mock pipeline — no real packaging runs)"
                    emitStageEvent(
                        eventType : 'PACKAGE_SUCCESS',
                        stage     : 'Package',
                        status    : 'SUCCESS',
                        fileName  : 'package_success_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        emitStageEvent(
                            eventType : 'PACKAGE_FAILED',
                            stage     : 'Package',
                            status    : 'FAILURE',
                            fileName  : 'package_failed_event.json'
                        )
                    }
                }
            }
        }

        // ══════════════════════════════════════════════════════════════════
        // STAGE 5 — Deploy
        // Every 4th build only: scripted DEPLOY_FAILED followed by a
        // successful retry, to feed Change Failure Rate (DoraOperators.CfrAgg
        // counts both DEPLOY_FAILED and DEPLOY_SUCCESS in-window) without
        // wiping out LeadTimeProcessFn's per-commit state on every run —
        // Lead Time for Changes still emits normally on the other 3 of 4
        // builds.
        // ══════════════════════════════════════════════════════════════════
        stage('Deploy') {
            steps {
                script {
                    def isScriptedFailureBuild = (env.BUILD_NUMBER as Integer) % 4 == 0

                    emitStageEvent(
                        eventType : 'DEPLOY_STARTED',
                        stage     : 'Deploy',
                        status    : 'SUCCESS',
                        fileName  : 'deploy_started_event.json'
                    )

                    if (isScriptedFailureBuild) {
                        echo "Simulating deployment attempt #1 for ${env.SERVICE_NAME} (build #${env.BUILD_NUMBER} — every 4th build is scripted to fail)"
                        emitStageEvent(
                            eventType : 'DEPLOY_FAILED',
                            stage     : 'Deploy',
                            status    : 'FAILURE',
                            fileName  : 'deploy_failed_event.json'
                        )
                        echo "Retrying deployment for ${env.SERVICE_NAME}"
                        emitStageEvent(
                            eventType : 'DEPLOY_STARTED',
                            stage     : 'Deploy',
                            status    : 'SUCCESS',
                            fileName  : 'deploy_retry_started_event.json',
                            idSuffix  : 'retry'
                        )
                        echo "Simulating deployment attempt #2 for ${env.SERVICE_NAME} (succeeds)"
                    } else {
                        echo "Simulating deployment for ${env.SERVICE_NAME} (mock pipeline — no real deployment runs)"
                    }

                    emitStageEvent(
                        eventType : 'DEPLOY_SUCCESS',
                        stage     : 'Deploy',
                        status    : 'SUCCESS',
                        fileName  : 'deploy_success_event.json'
                    )
                }
            }
            post {
                failure {
                    script {
                        emitStageEvent(
                            eventType : 'DEPLOY_FAILED',
                            stage     : 'Deploy',
                            status    : 'FAILURE',
                            fileName  : 'deploy_failed_final_event.json',
                            idSuffix  : 'final'
                        )
                    }
                }
            }
        }
    }

    post {
        success {
            echo "✅ Pipeline completed successfully for ${env.SERVICE_NAME} — build started through deploy success"
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
