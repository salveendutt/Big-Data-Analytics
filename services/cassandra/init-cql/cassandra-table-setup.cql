CREATE KEYSPACE transactions 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE transactions;

CREATE TABLE dataset1 (
    step INT,
    type TEXT,
    amount DECIMAL,
    isFlaggedFraud INT,
    isFraud INT,
    nameDest TEXT,
    nameOrig TEXT,
    newbalanceDest DECIMAL,
    newbalanceOrig DECIMAL,
    oldbalanceDest DECIMAL,
    oldbalanceOrg DECIMAL,
    PRIMARY KEY (step, nameOrig)
);


CREATE TABLE dataset2 (
    distance_from_home DECIMAL,
    distance_from_last_transaction DECIMAL,
    fraud DECIMAL,
    online_order DECIMAL,
    ratio_to_median_purchase_price DECIMAL,
    repeat_retailer DECIMAL,
    used_chip DECIMAL,
    used_pin_number DECIMAL,
    PRIMARY KEY (distance_from_home, fraud)
);


CREATE TABLE dataset3 (
    amt DECIMAL,
    bin TEXT,
    customer_id TEXT,
    entry_mode TEXT,
    fraud INT,
    fraud_scenario INT,
    post_ts TEXT,
    terminal_id TEXT,
    transaction_id TEXT,
    PRIMARY KEY (transaction_id)
);