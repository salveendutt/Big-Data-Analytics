FROM python:3.11.9-slim
COPY --from=openjdk:8-jre-slim /usr/local/openjdk-8 /usr/local/openjdk-8

ENV JAVA_HOME=/usr/local/openjdk-8

RUN update-alternatives --install /usr/bin/java java /usr/local/openjdk-8/bin/java 1

WORKDIR /app

COPY ./services/streaming_processing /app

RUN pip install --no-cache-dir -r streaming_processing_requirements.txt

LABEL Name="streaming_processing"

CMD ["python", "streaming_processing.py"]