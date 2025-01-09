FROM python:3.13.0-slim

WORKDIR /app

COPY ./services/streaming_simulation /app

COPY ./datasets /app

RUN --mount=type=cache,target=/root/.cache/pip pip install -r streaming-requirements.txt

ENV FLASK_APP=app.py

LABEL Name="SteamingSimulation"

CMD ["python", "streaming_simulation.py"]