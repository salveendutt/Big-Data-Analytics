-- Create keyspace for fraud analytics
CREATE KEYSPACE IF NOT EXISTS fraud_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

-- Use the fraud_analytics keyspace
USE fraud_analytics;

-- Table for fraud statistics by transaction type
CREATE TABLE IF NOT EXISTS fraud_by_transaction_type (
    type text,
    total_transactions bigint,
    total_fraudulent bigint,
    avg_amount double,
    fraud_rate double,
    PRIMARY KEY (type)
);

-- Table for fraud statistics by amount bucket
CREATE TABLE IF NOT EXISTS fraud_by_amount_bucket (
    amount_bucket text,
    total_transactions bigint,
    total_fraudulent bigint,
    avg_amount double,
    fraud_rate double,
    PRIMARY KEY (amount_bucket)
);

-- Table for hourly fraud statistics
CREATE TABLE IF NOT EXISTS hourly_fraud_stats (
    hour int,
    total_transactions bigint,
    total_fraudulent bigint,
    avg_amount double,
    PRIMARY KEY (hour)
);

-- Table for high-risk customer analysis
CREATE TABLE IF NOT EXISTS high_risk_customers (
    customer_id text,
    total_transactions bigint,
    fraudulent_transactions bigint,
    total_amount double,
    fraud_rate double,
    PRIMARY KEY (customer_id)
);

-- Table for real-time predictions from multiple models
CREATE TABLE IF NOT EXISTS real_time_predictions (
    transaction_id text,
    prediction_timestamp timestamp,
    transaction_type text,
    amount float,
    customer_id text,
    model1_fraud_probability float,
    model2_fraud_probability float,
    model3_fraud_probability float,
    ensemble_fraud_probability float,
    is_fraud boolean,
    model_version text,
    PRIMARY KEY (transaction_id)
);



CREATE TABLE IF NOT EXISTS predictions1 (
    id UUID,
    transaction_id TEXT,
    fraud BOOLEAN,
    prediction INT,
    fraud_probability FLOAT,
    customer_id TEXT,
    amount FLOAT,
    amount_to_balance_ratio FLOAT,
    timestamp TIMESTAMP,
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS predictions2 (
    id UUID,
    fraud BOOLEAN,
    prediction INT,
    fraud_probability FLOAT,
    distance_from_home float,
    distance_from_last_transaction float,
    ratio_to_median_purchase_price float,
    repeat_retailer int,
    used_chip int,
    used_pin_number int,
    online_order int,
    timestamp TIMESTAMP,
    PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS predictions3 (
    id UUID,
    fraud BOOLEAN,
    customer_id TEXT,
    fraud_probability FLOAT,
    amount FLOAT,
    timestamp TIMESTAMP,
    PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS predictions4 (
    id UUID,
    transaction_id TEXT,
    customer_id TEXT,
    PRIMARY KEY (id)
);