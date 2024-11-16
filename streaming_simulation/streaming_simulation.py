from flask import Flask, Response
import csv
import time
import argparse

app = Flask(__name__)


def get_data(file_path, delay):
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        for row in reader:
            yield ",".join(row) + "\n"
            time.sleep(delay)


@app.route("/data")
def data():
    return Response(
        get_data(app.config["file_path"], app.config["delay"]), mimetype="text/csv"
    )


@app.route("/")
def root():
    return '<div><a href="/data">Click here to start streaming data</a></div>'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Streaming Simulation")
    parser.add_argument("file_path", type=str, help="File path, must be in csv format")
    parser.add_argument(
        "--delay",
        type=int,
        default=1,
        help="delay between each row, in seconds, default is 1 second",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="delay between each row, in seconds, default is 1 second",
    )
    parser.add_argument(
        "--address",
        type=str,
        default="127.0.0.1",
        help="delay between each row, in seconds, default is 1 second",
    )
    args = parser.parse_args()
    app.config["file_path"] = args.file_path
    app.config["delay"] = args.delay
    app.config["port"] = args.port
    app.config["address"] = args.address

    app.run(threaded=True, host=app.config["address"], port=app.config["port"])
