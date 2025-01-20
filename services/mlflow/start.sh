#!/bin/bash

# Start the cron service in the background
service cron start

tail -f /var/log/cron.log &

echo $BACKEND_STORE_URI
echo $ARTIFACT_ROOT

# Start the MLflow server
mlflow server --backend-store-uri sqlite:////mlruns/mlflow.db --artifacts-destination hdfs://namenode:8020/user/models/mlruns --default-artifact-root hdfs://namenode:8020/user/models/mlruns --host 0.0.0.0
# mlflow server --backend-store-uri sqlite:////app\/mlruns\/mlflow.db --default-artifact-root /app\/mlruns\/artifacts --host 0.0.0.0
# mlflow server --backend-store-uri file:/app\/mlruns --default-artifact-root /app\/mlartifacts --host 0.0.0.0
# mlflow server --host 0.0.0.0