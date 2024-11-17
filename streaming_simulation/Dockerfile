FROM python:3.13.0-slim

WORKDIR /app

COPY ./streaming_simulation /app

COPY ./datasets /app

RUN pip install --no-cache-dir -r streaming-requirements.txt

EXPOSE 5000

ENV FLASK_APP=app.py

LABEL Name="SteamingSimulation"

CMD ["python", "streaming_simulation.py"]