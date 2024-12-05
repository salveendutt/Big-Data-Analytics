FROM python:3.13.0-slim

WORKDIR /app

COPY ./services/batch_processing /app

COPY ./datasets /app

RUN pip install --no-cache-dir -r batch_processing_requirements.txt

LABEL Name="batch_processing"

CMD ["python", "batch_processing.py"]