import sys
import os


if "pytest" in sys.modules:
    datasets = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data.csv")
    ]
else:
    datasets = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "Fraud1.csv"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "Fraud2.csv"),
    ]

port = 5000
