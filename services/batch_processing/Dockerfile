FROM python:3.12.7-slim
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME=/usr/local/openjdk-8

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

RUN apt-get update && apt-get install -y procps git

WORKDIR /app

COPY ./services/batch_processing /app

RUN pip install --no-cache-dir -r batch_processing_requirements.txt

LABEL Name="batch_processing"

CMD ["python", "batch_processing.py"]