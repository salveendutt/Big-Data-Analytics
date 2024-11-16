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
    return Response(get_data(args.file_path, args.delay), mimetype="text/csv")


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
    args = parser.parse_args()

    app.run(debug=True, threaded=True)
