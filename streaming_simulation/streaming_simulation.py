from flask import Flask, Response
import csv
import time
import argparse
from config_streaming_simulation import datasets, port
import random

app = Flask(__name__)


readers = [csv.reader(open(dataset, "r")) for dataset in datasets]


def get_data(slug, delay):
    for row in readers[slug]:
        yield ",".join(row) + "\n"
        time.sleep(delay)


@app.route("/data/<int:slug>")
def data(slug):
    if slug < 0 or slug >= len(datasets):
        return "Invalid dataset", 400
    reader = readers[slug]
    random_row = random.choice(reader)
    return ",".join(random_row) + "\n"


@app.route("/stream/<int:slug>")
def stream(slug):
    if slug < 0 or slug >= len(datasets):
        return "Invalid dataset", 400
    return Response(get_data(slug, app.config["delay"]), mimetype="text/csv")


@app.route("/")
def root():
    return '<div><a href="/data/0">Click here to start streaming data</a></div>'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Streaming Simulation")
    parser.add_argument(
        "--delay",
        type=int,
        default=1,
        help="delay between each row, in seconds, default is 1 second",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=port,
        help="port number, default is saved in config_streaming_simulation.py",
    )
    parser.add_argument(
        "--address",
        type=str,
        default="0.0.0.0",
        help="address, default is 0.0.0.0",
    )
    args = parser.parse_args()
    app.config["delay"] = args.delay
    app.config["port"] = args.port
    app.config["address"] = args.address

    app.run(threaded=True, host=app.config["address"], port=app.config["port"])
