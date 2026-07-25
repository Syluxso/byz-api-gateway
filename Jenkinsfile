pipeline {
    agent any

    environment {
        DEPLOY_DIR = '/opt/services/byz-api-gateway'
        BINARY_NAME = 'byz-api-gateway'
        SUPERVISOR_PROGRAM = 'byz-api-gateway'
    }

    stages {
        stage('Build') {
            steps {
                sh '''
                    set -e
                    go version
                    CGO_ENABLED=0 go build -o "${BINARY_NAME}" .
                    test -x "./${BINARY_NAME}"
                '''
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    set -e
                    sudo mkdir -p "${DEPLOY_DIR}"
                    sudo supervisorctl stop "${SUPERVISOR_PROGRAM}" || true
                    # Legacy Java process names
                    sudo supervisorctl stop byz-gateway || true
                    sudo supervisorctl stop gateway || true
                    sudo cp "./${BINARY_NAME}" "${DEPLOY_DIR}/${BINARY_NAME}"
                    sudo chmod 755 "${DEPLOY_DIR}/${BINARY_NAME}"
                    sudo chown root:root "${DEPLOY_DIR}/${BINARY_NAME}"
                    if [ -f ./start.sh ]; then
                        sudo cp ./start.sh "${DEPLOY_DIR}/start.sh"
                        sudo chmod 755 "${DEPLOY_DIR}/start.sh"
                        sudo chown root:root "${DEPLOY_DIR}/start.sh"
                    fi
                    sudo supervisorctl reread
                    sudo supervisorctl update "${SUPERVISOR_PROGRAM}" || true
                    sudo supervisorctl start "${SUPERVISOR_PROGRAM}" || sudo supervisorctl restart "${SUPERVISOR_PROGRAM}"
                    sleep 3
                    sudo supervisorctl status "${SUPERVISOR_PROGRAM}" || true
                    curl -sf "http://127.0.0.1:8096/actuator/health" || true
                '''
            }
        }
    }

    post {
        success {
            echo 'byz-api-gateway deployed (Go edge)'
        }
        failure {
            echo 'Build or deploy failed. Ensure Go, Redis, and sudoers allow deploy.'
        }
    }
}
