FROM python:3.12.7-slim
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME=/usr/local/openjdk-8

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

RUN apt-get update && apt-get install -y procps git

WORKDIR /app

COPY ./services/streaming_processing /app

# COPY ./datasets /app/datasets

# COPY ./datasets/train_Fraud.csv /app/datasets/train_Fraud.csv
# COPY ./datasets/train_Credit_Card_Fraud_.csv /app/datasets/train_Credit_Card_Fraud_.csv
# COPY ./datasets/train_transactions_df.csv /app/datasets/train_transactions_df.csv



RUN --mount=type=cache,target=/root/.cache/pip pip install -r streaming_processing_requirements.txt

LABEL Name="streaming_processing"

CMD ["python", "streaming_processing.py"]