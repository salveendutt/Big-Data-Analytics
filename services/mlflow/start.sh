#!/bin/bash

# Start the cron service in the background
service cron start

tail -f /var/log/cron.log &

# Start the MLflow server
mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root /mlflow/artifacts --host 0.0.0.0
