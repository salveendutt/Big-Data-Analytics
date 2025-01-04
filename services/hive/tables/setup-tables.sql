SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

create database if not exists fraud;
use fraud;

CREATE EXTERNAL TABLE if not exists dataset1 (
    step INT,
    type STRING,
    amount FLOAT,
    nameOrig STRING,
    oldbalanceOrg FLOAT,
    newbalanceOrig FLOAT,
    nameDest STRING,
    oldbalanceDest FLOAT,
    newbalanceDest FLOAT,
    isFraud INT,
    isFlaggedFraud INT
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dataset1';

CREATE EXTERNAL TABLE if not exists dataset2 (
    distance_from_home FLOAT,
    distance_from_last_transaction FLOAT,
    fraud INT,
    ratio_to_median_purchase_price FLOAT,
    repeat_retailer INT,
    used_chip INT,
    used_pin_number INT,
    online_order INT
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dataset2';

CREATE EXTERNAL TABLE if not exists dataset3 (
    amt FLOAT,
    bin INT,
    customer_id STRING,
    entry_mode STRING,
    fraud INT,
    fraud_scenario INT,
    post_ts STRING,
    terminal_id STRING,
    transaction_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dataset3';

msck repair table dataset1;
msck repair table dataset2;
msck repair table dataset3;