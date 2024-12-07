import unittest
from utils_streaming_processing import (
    preprocess_row_1,
    preprocess_row_2,
    preprocess_row_3,
    preprocess_row_4,
)


class SteamingProcessingTestCase(unittest.TestCase):
    def setUp(self):
        self.example_value = 0

    def test_data_processing_example(self):
        self.assertEqual(self.example_value, 0)

    def test_preprocess_1_payment(self):
        raw = {
            "step": 1,
            "type": "PAYMENT",
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "nameDest": "M1979787155",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        expected = {
            "type": 4,
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "isMerchant": 1,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_1(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_1_cash_in(self):
        raw = {
            "step": 1,
            "type": "CASH-IN",
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "nameDest": "B1979787155",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        expected = {
            "type": 1,
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "isMerchant": 0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_1(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_1_cash_out(self):
        raw = {
            "step": 1,
            "type": "CASH-OUT",
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "nameDest": "B1979787155",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        expected = {
            "type": 2,
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "isMerchant": 0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_1(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_1_debit(self):
        raw = {
            "step": 1,
            "type": "DEBIT",
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "nameDest": "B1979787155",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        expected = {
            "type": 3,
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "isMerchant": 0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_1(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_1_debit(self):
        raw = {
            "step": 1,
            "type": "TRANSFER",
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "nameDest": "B1979787155",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        expected = {
            "type": 5,
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "isMerchant": 0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_1(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_1_unknown(self):
        raw = {
            "step": 1,
            "type": "UNKNOWN",
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "nameDest": "B1979787155",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        expected = {
            "type": 0,
            "amount": 9839.64,
            "oldbalanceOrg": 170136.0,
            "newbalanceOrig": 160296.36,
            "isMerchant": 0,
            "isFlaggedFraud": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_1(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_row_2(self):
        raw = {
            "distance_from_home": 57.877857,
            "distance_from_last_transaction": 0.311140,
            "ratio_to_median_purchase_price": 1.945940,
            "repeat_retailer": 1.0,
            "used_chip": 1.0,
            "used_pin_number": 0.0,
            "fraud": 0.0,
        }
        expected = {
            "distance_from_home": 57.877857,
            "distance_from_last_transaction": 0.311140,
            "ratio_to_median_purchase_price": 1.945940,
            "repeat_retailer": 1,
            "used_chip": 1,
            "used_pin_number": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_2(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_3_contactless(self):
        raw = {
            "transaction_id": "OyWUo6ruReKft",
            "post_ts": "2023-02-01 00:00:30",
            "customer_id": "C00005143",
            "bin": 424208,
            "terminal_id": "T001014",
            "amt": 38.97,
            "entry_mode": "Contactless",
            "fraud": 0,
            "fraud_scenario": 0,
        }
        expected = {
            "customer_id": "C00005143",
            "bin": 424208,
            "amount": 38.97,
            "entry_mode": 1,
            "isFraud": 0,
        }
        actual = preprocess_row_3(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_3_chip(self):
        raw = {
            "transaction_id": "OyWUo6ruReKft",
            "post_ts": "2023-02-01 00:00:30",
            "customer_id": "C00005143",
            "bin": 424208,
            "terminal_id": "T001014",
            "amt": 38.97,
            "entry_mode": "Chip",
            "fraud": 0,
            "fraud_scenario": 0,
        }
        expected = {
            "customer_id": "C00005143",
            "bin": 424208,
            "amount": 38.97,
            "entry_mode": 2,
            "isFraud": 0,
        }
        actual = preprocess_row_3(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_3_swipe(self):
        raw = {
            "transaction_id": "OyWUo6ruReKft",
            "post_ts": "2023-02-01 00:00:30",
            "customer_id": "C00005143",
            "bin": 424208,
            "terminal_id": "T001014",
            "amt": 38.97,
            "entry_mode": "Swipe",
            "fraud": 0,
            "fraud_scenario": 0,
        }
        expected = {
            "customer_id": "C00005143",
            "bin": 424208,
            "amount": 38.97,
            "entry_mode": 3,
            "isFraud": 0,
        }
        actual = preprocess_row_3(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_3_unknown(self):
        raw = {
            "transaction_id": "OyWUo6ruReKft",
            "post_ts": "2023-02-01 00:00:30",
            "customer_id": "C00005143",
            "bin": 424208,
            "terminal_id": "T001014",
            "amt": 38.97,
            "entry_mode": "UNKNOWN",
            "fraud": 0,
            "fraud_scenario": 0,
        }
        expected = {
            "customer_id": "C00005143",
            "bin": 424208,
            "amount": 38.97,
            "entry_mode": 0,
            "isFraud": 0,
        }
        actual = preprocess_row_3(raw)
        self.assertEqual(actual, expected)

    def test_preprocess_row_4(self):
        raw = {
            "Time": 57.877857,
            "V1": 0.311140,
            "V2": 1.945940,
            "V3": 1.0,
            "V4": 1.0,
            "V5": 0.0,
            "Amount": 149.62,
            "Class": 0,
        }
        expected = {
            "V1": 0.311140,
            "V2": 1.945940,
            "V3": 1.0,
            "V4": 1.0,
            "V5": 0.0,
            "amount": 149.62,
            "isFraud": 0,
        }
        actual = preprocess_row_4(raw)
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
