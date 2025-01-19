#!/bin/bash

# Start the cron service in the background
service cron start

tail -f /var/log/cron.log &

# Start the MLflow server
# mlflow server --backend-store-uri $BACKEND_STORE_URI --default-artifact-root $ARTIFACT_ROOT --host 0.0.0.0
mlflow server --backend-store-uri sqlite:///mlflow\mlflow.db --default-artifact-root /mlflow/artifacts --host 0.0.0.0
