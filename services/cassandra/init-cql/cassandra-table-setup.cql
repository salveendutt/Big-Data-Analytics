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

-- Table for real-time fraud predictions
CREATE TABLE IF NOT EXISTS real_time_predictions (
    transaction_id text,
    prediction_timestamp timestamp,
    transaction_type text,
    amount double,
    customer_id text,
    fraud_probability double,
    is_fraud boolean,
    model_version text,
    PRIMARY KEY (transaction_id)
);