import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import joblib

fraud_data = pd.read_csv("../datasets/test_Fraud.csv")
credit_card_data = pd.read_csv("../datasets/test_Credit_Card_Fraud_.csv")
transactions_data = pd.read_csv("../datasets/test_transactions_df.csv")

fraud_data["amount_to_balance_ratio"] = fraud_data.apply(
    lambda row: row["amount"] / row["oldbalanceOrg"] if row["oldbalanceOrg"] > 0 else 0,
    axis=1,
)
y_fraud = fraud_data["isFraud"]
X_fraud = fraud_data[["amount", "amount_to_balance_ratio"]]

y_credit_card = credit_card_data["fraud"]
X_credit_card = credit_card_data[
    [
        "distance_from_home",
        "distance_from_last_transaction",
        "ratio_to_median_purchase_price",
        "repeat_retailer",
        "used_chip",
        "used_pin_number",
        "online_order",
    ]
]

y_transactions = transactions_data["fraud"]
X_transactions = transactions_data[["amt"]]

rf_fraud = RandomForestClassifier(n_estimators=10, random_state=2025)
rf_fraud.fit(X_fraud, y_fraud)

rf_credit_card = RandomForestClassifier(n_estimators=10, random_state=2025)
rf_credit_card.fit(X_credit_card, y_credit_card)

rf_transactions = RandomForestClassifier(n_estimators=10, random_state=2025)
rf_transactions.fit(X_transactions, y_transactions)

joblib.dump(rf_fraud, "rf_fraud_model.joblib")
joblib.dump(
    rf_credit_card,
    "rf_credit_card_model.joblib",
)
joblib.dump(
    rf_transactions,
    "rf_transactions_model.joblib",
)

import shutil

for file in [
    "rf_fraud_model.joblib",
    "rf_credit_card_model.joblib",
    "rf_transactions_model.joblib",
]:
    shutil.move(file, f"../services/streaming_processing/models/{file}")
