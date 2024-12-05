FROM python:3.13.0-slim

WORKDIR /app

COPY ./services/streaming_processing /app

COPY ./datasets /app

RUN pip install --no-cache-dir -r streaming_processing_requirements.txt

LABEL Name="streaming_processing"

CMD ["python", "streaming_processing.py"]