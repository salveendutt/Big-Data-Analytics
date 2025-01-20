FROM python:3.12.7-slim

COPY --from=openjdk:11-jre-slim /usr/local/openjdk-11 /usr/local/openjdk-11

# Set working directory
WORKDIR /app

ENV JAVA_HOME=/usr/local/openjdk-11
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PATH="${JAVA_HOME}/bin:${PATH}"


ENV BACKEND_STORE_URI=sqlite:////mlruns/mlflow.db
ENV ARTIFACT_ROOT=file:///mlruns

# Install necessary dependencies
RUN apt-get update && apt-get install -y cron procps  && \
    pip install setuptools mlflow pyspark

# Expose the port for the MLflow server
EXPOSE 5000

# Copy your services and scripts
COPY ./services/mlflow /app

# COPY ./services/mlflow/hadoop /hadoop

ENV HADOOP_HOME=/hadoop
ENV PATH="${HADOOP_HOME}/bin:${PATH}"

COPY ./datasets/train_Fraud.csv /app/train_Fraud.csv

COPY ./services/mlflow/crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

RUN touch /var/log/cron.log && chmod 666 /var/log/cron.log

RUN mkdir /mlruns
RUN chmod -R 777 /mlruns

RUN chmod -R 777 /app

RUN chmod +x ./start.sh

CMD ["./start.sh"]
