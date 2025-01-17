import sys
import os


if "pytest" in sys.modules:
    datasets = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_data.csv")
    ]
else:
    datasets = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_Fraud.csv"),
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_Credit_Card_Fraud_.csv"
        ),
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_transactions_df.csv"
        ),
    ]

port = 5001
