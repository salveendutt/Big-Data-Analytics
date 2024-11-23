import sys
import os


if "pytest" in sys.modules:
    datasets = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data.csv")
    ]
else:
    datasets = ["Fraud1.csv", "Fraud2.csv"]

port = 5000
