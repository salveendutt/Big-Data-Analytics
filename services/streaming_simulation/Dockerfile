FROM python:3.13.0-slim

WORKDIR /app

COPY ./services/streaming_simulation /app

COPY ./datasets/test_transactions_df.csv /app/test_transactions_df.csv

COPY ./datasets/test_Credit_Card_Fraud_.csv /app/test_Credit_Card_Fraud_.csv

COPY ./datasets/test_Fraud.csv /app/test_Fraud.csv

RUN --mount=type=cache,target=/root/.cache/pip pip install -r streaming-requirements.txt

ENV FLASK_APP=app.py

LABEL Name="SteamingSimulation"

CMD ["python", "streaming_simulation.py"]