SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


create database if not exists fraud;
use fraud;

CREATE EXTERNAL TABLE if not exists dataset1 (
    step INT,
    type STRING,
    amount DOUBLE,
    isFlaggedFraud INT,
    isFraud INT,
    nameDest STRING,
    nameOrig STRING,
    newbalanceDest DOUBLE,
    newbalanceOrig DOUBLE,
    oldbalanceDest DOUBLE,
    oldbalanceOrg DOUBLE
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dataset1';


CREATE EXTERNAL TABLE if not exists dataset2 (
    distance_from_home DOUBLE,
    distance_from_last_transaction DOUBLE,
    fraud INT,
    online_order INT,
    ratio_to_median_purchase_price DOUBLE,
    repeat_retailer INT,
    used_chip INT,
    used_pin_number INT
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dataset2';


CREATE EXTERNAL TABLE if not exists dataset3 (
    amt DOUBLE,
    bin STRING,
    customer_id STRING,
    entry_mode STRING,
    fraud INT,
    fraud_scenario INT,
    post_ts TIMESTAMP,
    terminal_id STRING,
    transaction_id STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/dataset3';



