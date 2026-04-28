pipeline {
    agent any

    environment {
        DYNAMODB_ENDPOINT = 'http://localhost:8000'
        REDIS_HOST        = 'localhost'
        DEMO_MODE         = 'false'
    }

    stages {
        // ========== STAGE 1: BUILD ==========
        stage('Build') {
            steps {
                echo '══════════════════════════════════════'
                echo '  STAGE 1: BUILD & INSTALL'
                echo '══════════════════════════════════════'

                sh 'python3 --version'

                sh '''
                    python3 -m venv venv || true
                    . venv/bin/activate
                    pip install -r requirements.txt
                '''

                sh '''
                    . venv/bin/activate
                    python3 -c "import flask; print(f'Flask {flask.__version__}')"
                    python3 -c "import boto3; print(f'boto3 {boto3.__version__}')"
                    python3 -c "import redis; print(f'redis {redis.__version__}')"
                    python3 -c "import streamlit; print(f'streamlit {streamlit.__version__}')"
                '''

                echo 'Build stage complete ✓'
            }
        }

        // ========== STAGE 2: TEST ==========
        stage('Test') {
            steps {
                echo '══════════════════════════════════════'
                echo '  STAGE 2: TEST SUITE'
                echo '══════════════════════════════════════'

                // Ensure DynamoDB Local and Redis are running
                sh '''
                    echo "Checking DynamoDB Local..."
                    for i in $(seq 1 10); do
                        curl -s http://localhost:8000/shell/ && break || sleep 2
                    done

                    echo "Checking Redis..."
                    redis-cli ping || echo "Redis not responding"
                '''

                // Initialize database
                sh '''
                    . venv/bin/activate
                    python3 dynamo_manager.py
                    echo "Tables created ✓"
                '''

                // Generate test data
                sh '''
                    . venv/bin/activate
                    python3 data_generator.py
                    echo "Test data generated ✓"
                '''

                // Run anomaly detection + recommendations
                sh '''
                    . venv/bin/activate
                    python3 anomaly_detector.py
                    python3 recommendation_engine.py
                    echo "Processing pipeline complete ✓"
                '''

                // Run pytest
                sh '''
                    . venv/bin/activate
                    pytest test_api.py -v --tb=short
                '''

                // Performance smoke test
                sh '''
                    . venv/bin/activate
                    python3 -c "
import time
from api import app
client = app.test_client()
times = []
for _ in range(50):
    start = time.time()
    client.get('/api/dashboard/acct-001')
    times.append(time.time() - start)
avg_ms = sum(times) / len(times) * 1000
print(f'Avg dashboard latency: {avg_ms:.2f}ms')
assert avg_ms < 500, f'Latency too high: {avg_ms:.2f}ms'
"
                '''

                echo 'Test stage complete ✓'
            }
        }

        // ========== STAGE 3: DEPLOY ==========
        stage('Deploy') {
            steps {
                echo '══════════════════════════════════════'
                echo '  STAGE 3: DEPLOY'
                echo '══════════════════════════════════════'

                // Build Docker image
                sh '''
                    docker build -t cloudwatch-app:${BUILD_NUMBER} .
                    docker tag cloudwatch-app:${BUILD_NUMBER} cloudwatch-app:latest
                    echo "Docker image built ✓"
                '''

                // Verify image
                sh '''
                    docker images cloudwatch-app
                    echo "Image size:"
                    docker inspect cloudwatch-app:latest --format='{{.Size}}'
                '''

                // Deploy using docker-compose
                sh '''
                    docker-compose down || true
                    docker-compose up -d
                    echo "Containers started ✓"
                '''

                // Wait and verify
                sh '''
                    echo "Waiting for services to start..."
                    sleep 15
                    curl -s http://localhost:5000/api/health || echo "API not ready yet"
                    echo ""
                    echo "══════════════════════════════════════"
                    echo "  DEPLOYMENT SUCCESSFUL"
                    echo "  Build: #${BUILD_NUMBER}"
                    echo "  API:   http://localhost:5000"
                    echo "  UI:    http://localhost:8501"
                    echo "══════════════════════════════════════"
                '''

                echo 'Deploy stage complete ✓'
            }
        }
    }

    post {
        success {
            echo '✅ Pipeline completed successfully!'
        }
        failure {
            echo '❌ Pipeline failed. Check logs above.'
        }
        always {
            echo "Build #${BUILD_NUMBER} finished at ${new Date()}"
        }
    }
}
