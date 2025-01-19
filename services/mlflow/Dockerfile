FROM python:3.12.7-slim

COPY --from=openjdk:11-jre-slim /usr/local/openjdk-11 /usr/local/openjdk-11

ENV JAVA_HOME=/usr/local/openjdk-11
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ENV MLFLOW_TRACKING_URI="file:/mlflow/artifacts"
# ENV BACKEND_STORE_URI="sqlite:///mlflow/mlflow.db"
# ENV ARTIFACT_ROOT="/mlflow/artifacts"

# ENV MLFLOW_RUNS_DIR=/mlflow
# ENV BACKEND_STORE_DB_PATH=$MLFLOW_RUNS_DIR/mlflow.db
# ENV BACKEND_STORE_URI=sqlite://$BACKEND_STORE_DB_PATH
# ENV DEFAULT_ARTIFACT_ROOT=$MLFLOW_RUNS_DIR/artifacts
# ENV EXTRA_MLFLOW_PARAMS=''

ENV MLFLOW_TRACKING_URI="file:///mlflow"
ENV BACKEND_STORE_URI="sqlite:///mlflow\mlflow.db"
ENV ARTIFACT_ROOT="/mlflow/artifacts"

# RUN chmod -R 777 /mlflow

# Set working directory
WORKDIR /app

# Install necessary dependencies
RUN apt-get update && apt-get install -y cron procps  && \
    pip install setuptools mlflow pyspark

# Expose the port for the MLflow server
EXPOSE 5000

# Copy your services and scripts
COPY ./services/mlflow /app

COPY ./services/mlflow/crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

RUN touch /var/log/cron.log && chmod 666 /var/log/cron.log

RUN chmod +x ./start.sh

RUN mkdir -p /mlflow && \
    chmod -R 777 /mlflow

CMD ["./start.sh"]
