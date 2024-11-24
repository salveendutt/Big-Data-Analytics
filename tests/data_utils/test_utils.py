import pytest
import sys
import os

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from src.data_utils.utils import preprocess_row_1, preprocess_row_2, preprocess_row_3, preprocess_row_4

def test_preprocess_row_1():
    raw = {
        'step': 1,
        'type': 'PAYMENT',
        'amount': 9839.64,
        'oldbalanceOrg': 170136.0,
        'newbalanceOrig': 160296.36,
        'nameDest': 'M1979787155',
        'oldbalanceDest': 0.0,
        'newbalanceDest': 0.0,
        'isFlaggedFraud': 0,
        'isFraud': 0
    }
    expected = {
        'type': 4,
        'amount': 9839.64,
        'oldbalanceOrg': 170136.0,
        'newbalanceOrig': 160296.36,
        'isMerchant': 1,
        'isFlaggedFraud': 0,
        'isFraud': 0
    }
    actual = preprocess_row_1(raw)
    assert actual == expected

def test_preprocess_row_2():
    raw = {
        'distance_from_home': 57.877857,
        'distance_from_last_transaction': 0.311140,
        'ratio_to_median_purchase_price': 1.945940,
        'repeat_retailer': 1.0,
        'used_chip': 1.0,
        'used_pin_number': 0.0,
        'fraud': 0.0
    }
    expected = {
        'distance_from_home': 57.877857,
        'distance_from_last_transaction': 0.311140,
        'ratio_to_median_purchase_price': 1.945940,
        'repeat_retailer': 1,
        'used_chip': 1,
        'used_pin_number': 0,
        'isFraud': 0
    }
    actual = preprocess_row_2(raw)
    assert actual == expected

def test_preprocess_row_3():
    raw = {
        'transaction_id': 'OyWUo6ruReKft',
        'post_ts': '2023-02-01 00:00:30',
        'customer_id': 'C00005143',
        'bin': 424208,
        'terminal_id': 'T001014',
        'amt': 38.97,
        'entry_mode': 'Contactless',
        'fraud': 0, 
        'fraud_scenario': 0
    }
    expected = {
        'customer_id': 'C00005143',
        'bin': 424208,
        'amount': 38.97,
        'entry_mode': 1,
        'isFraud': 0
    }
    actual = preprocess_row_3(raw)
    assert actual == expected

def test_preprocess_row_4():
    raw = {
        'Time': 57.877857,
        'V1': 0.311140,
        'V2': 1.945940,
        'V3': 1.0,
        'V4': 1.0,
        'V5': 0.0,
        'Amount': 149.62,
        'Class': 0
    }
    expected = {
        'V1': 0.311140,
        'V2': 1.945940,
        'V3': 1.0,
        'V4': 1.0,
        'V5': 0.0,
        'amount': 149.62,
        'isFraud': 0
    }
    actual = preprocess_row_4(raw)
    assert actual == expected
