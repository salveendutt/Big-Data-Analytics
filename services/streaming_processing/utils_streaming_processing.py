import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    from_json,
    lit,
    hour,
    to_timestamp,
    concat,
    count,
    avg,
    sum,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
    DoubleType,
)
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from kafka import KafkaConsumer, KafkaProducer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
from typing import Dict, List
from datetime import datetime
import config_streaming_processing as config
import logging
from kafka.errors import NoBrokersAvailable
import numpy as np
import uuid
import pandas as pd
from sklearn.model_selection import train_test_split


class FraudDetectionPipeline:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        # Logging
        self.logger = logging.getLogger(__name__)
        self.timestamp_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        # Spark
        self.spark = self._initialize_spark()
        self._initialize_schemas()
        # Kafka
        self.consumers = self._initialize_kafka_consumers()
        # Cassandra
        self.cassandra_session = self._initialize_cassandra()

        # Initialize models
        self.model1, self.model2, self.model3 = self.load_latest_models_from_hdfs()
        self.model_version = "latest" if self.model1 else "1.0.0"

    def _initialize_spark(self):
        """Initialize Spark session"""
        try:
            return (
                SparkSession.builder.master(config.spark_master_address)
                .config(
                    "spark.sql.warehouse.dir",
                    "hdfs://namenode:8020/user/hive/warehouse",
                )
                .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
                .config("spark.jars.packages", config.spark_jars_packages)
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .appName("Fraud Detection Pipeline")
                .getOrCreate()
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {e}")
            raise

    def _initialize_kafka_consumers(self):
        """Initialize Kafka consumers with retry logic"""
        consumers = {}
        for attempt in range(config.kafka_connection_attempts):
            try:
                for topic in config.kafka_topics:
                    consumers[topic] = KafkaConsumer(
                        topic,
                        bootstrap_servers=config.kafka_address,
                        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                        group_id=f"fraud-detection-group-{topic}",
                        auto_offset_reset="latest",
                    )
                self.logger.info("Successfully connected to Kafka consumers")
                return consumers
            except NoBrokersAvailable:
                if attempt < config.kafka_connection_attempts - 1:
                    self.logger.warning(
                        f"Failed to connect to Kafka, attempt {attempt + 1} of {config.kafka_connection_attempts}"
                    )
                    time.sleep(config.kafka_connection_attempts_delay)
                else:
                    self.logger.error("Failed to connect to Kafka after all attempts")
                    raise

    def _initialize_cassandra(self):
        """Initialize connection to Cassandra"""
        try:
            cluster = Cluster(
                ["cassandra"],
                auth_provider=PlainTextAuthProvider(
                    username=config.cassandra_username,
                    password=config.cassandra_password,
                ),
            )
            session = cluster.connect()
            session.set_keyspace(config.cassandra_keyspace)
            return session
        except Exception as e:
            self.logger.error(f"Error connecting to Cassandra: {str(e)}")
            return None

    def _initialize_schemas(self):
        """Initialize schemas for each dataset"""
        self.schema1 = StructType(
            [
                StructField("step", IntegerType()),
                StructField("type", StringType()),
                StructField("amount", FloatType()),
                StructField("nameOrig", StringType()),
                StructField("oldbalanceOrg", FloatType()),
                StructField("newbalanceOrg", FloatType()),
                StructField("nameDest", StringType()),
                StructField("oldbalanceDest", FloatType()),
                StructField("newbalanceDest", FloatType()),
                StructField("isFraud", IntegerType()),
                StructField("isFlaggedFraud", IntegerType()),
            ]
        )

        self.schema2 = StructType(
            [
                StructField("distance_from_home", FloatType()),
                StructField("distance_from_last_transaction", FloatType()),
                StructField("fraud", IntegerType()),
                StructField("ratio_to_median_purchase_price", FloatType()),
                StructField("repeat_retailer", IntegerType()),
                StructField("used_chip", IntegerType()),
                StructField("used_pin_number", IntegerType()),
                StructField("online_order", IntegerType()),
            ]
        )

        self.schema3 = StructType(
            [
                StructField("amt", FloatType()),
                StructField("bin", IntegerType()),
                StructField("customer_id", StringType()),
                StructField("entry_mode", StringType()),
                StructField("fraud", IntegerType()),
                StructField("fraud_scenario", IntegerType()),
                StructField("post_ts", StringType()),
                StructField("terminal_id", StringType()),
                StructField("transaction_id", StringType()),
            ]
        )

    def create_feature_vector1(self, df):
        """Create feature vector for model1 (dataset1)"""
        try:
            # Add transaction_id column if it doesn't exist
            df = df.withColumn(
                "transaction_id",
                when(
                    col("nameOrig").isNotNull(),
                    concat(col("nameOrig"), lit("_"), col("step").cast("string")),
                ).otherwise(lit(str(uuid.uuid4()))),
            )

            feature_cols = ["amount", "amount_to_balance_ratio"]

            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

            # Select all necessary columns including those needed for predictions
            return assembler.transform(df).select(
                "features",
                "fraud",
                "transaction_id",
                "type",
                "amount",
                "nameOrig",
                "nameDest",
            )
        except Exception as e:
            self.logger.error(f"Error creating feature vector for model1: {str(e)}")
            raise

    def create_feature_vector2(self, df):
        """Create feature vector for model2 (dataset2)"""
        try:
            feature_cols = [
                "distance_from_home",
                "distance_from_last_transaction",
                "ratio_to_median_purchase_price",
                "repeat_retailer",
                "used_chip",
                "used_pin_number",
                "online_order",
            ]

            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

            return assembler.transform(df).select("features", "fraud")
        except Exception as e:
            self.logger.error(f"Error creating feature vector for model2: {str(e)}")
            raise

    def create_feature_vector3(self, df):
        """Create feature vector for model3 (dataset3)"""
        try:
            feature_cols = ["amt"]

            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

            return assembler.transform(df).select(
                "features", "fraud", "customer_id", "transaction_id"
            )
        except Exception as e:
            self.logger.error(f"Error creating feature vector for model3: {str(e)}")
            raise

    def train_models(self, training_data1, training_data2, training_data3):
        """Train all three models using their respective datasets"""
        try:
            # Convert training data to DataFrames
            self.logger.info("Creating dataframes")
            train_data1 = self.spark.createDataFrame(training_data1)
            train_data2 = self.spark.createDataFrame(training_data2)
            train_data3 = self.spark.createDataFrame(training_data3)

            # Preprocess training data
            self.logger.info("Preprocessing datasets")
            train_data1 = self.preprocess_dataset1(train_data1)
            train_data2 = self.preprocess_dataset2(train_data2)
            train_data3 = self.preprocess_dataset3(train_data3)

            # Create feature vectors
            self.logger.info("Creating feature vectors")
            train_data1 = self.create_feature_vector1(train_data1)
            train_data2 = self.create_feature_vector2(train_data2)
            train_data3 = self.create_feature_vector3(train_data3)

            # Initialize models
            rf1 = RandomForestClassifier(
                labelCol="fraud", featuresCol="features", numTrees=10
            )
            rf2 = RandomForestClassifier(
                labelCol="fraud", featuresCol="features", numTrees=10
            )
            rf3 = RandomForestClassifier(
                labelCol="fraud", featuresCol="features", numTrees=10
            )

            # Create pipelines
            pipeline1 = Pipeline(stages=[rf1])
            pipeline2 = Pipeline(stages=[rf2])
            pipeline3 = Pipeline(stages=[rf3])

            # Train models
            self.logger.info("Training on dataset1")
            self.model1 = pipeline1.fit(train_data1)
            self.logger.info("Training on dataset2")
            self.model2 = pipeline2.fit(train_data2)
            self.logger.info("Training on dataset3")
            self.model3 = pipeline3.fit(train_data3)
            self.logger.info("Training completed")

            # Save models with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Save models to HDFS with proper paths
            base_path = "hdfs://namenode:8020/user/models/fraud_detection"

            # Ensure base directory exists
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            base_path_hdfs = self.spark._jvm.org.apache.hadoop.fs.Path(base_path)
            if not fs.exists(base_path_hdfs):
                fs.mkdirs(base_path_hdfs)

            # Save each model
            model1_path = f"{base_path}/model1_{timestamp}"
            model2_path = f"{base_path}/model2_{timestamp}"
            model3_path = f"{base_path}/model3_{timestamp}"

            self.model1.write().overwrite().save(model1_path)
            self.model2.write().overwrite().save(model2_path)
            self.model3.write().overwrite().save(model3_path)

            self.model_version = timestamp
            self.logger.info(
                f"Successfully trained and saved models version {timestamp}"
            )

        except Exception as e:
            self.logger.error(f"Error training models: {str(e)}")
            raise

    def load_latest_models_from_hdfs(self):
        """Load the latest models from HDFS"""
        try:
            base_path = "hdfs://namenode:8020/user/models/fraud_detection"

            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            base_path_hdfs = self.spark._jvm.org.apache.hadoop.fs.Path(base_path)

            if not fs.exists(base_path_hdfs):
                self.logger.info("No existing models found in HDFS")
                return None, None, None

            model_dirs = []
            for status in fs.listStatus(base_path_hdfs):
                if status.isDirectory():
                    model_dirs.append(status.getPath().getName())

            if not model_dirs:
                self.logger.info("No model directories found")
                return None, None, None

            timestamps = set()
            for dir_name in model_dirs:
                parts = dir_name.split("_")
                if len(parts) >= 2:
                    timestamps.add(parts[-1])

            if not timestamps:
                self.logger.info("No valid model timestamps found")
                return None, None, None

            latest_timestamp = max(timestamps)

            model1_path = f"{base_path}/model1_{latest_timestamp}"
            model2_path = f"{base_path}/model2_{latest_timestamp}"
            model3_path = f"{base_path}/model3_{latest_timestamp}"

            model1_exists = fs.exists(
                self.spark._jvm.org.apache.hadoop.fs.Path(model1_path)
            )
            model2_exists = fs.exists(
                self.spark._jvm.org.apache.hadoop.fs.Path(model2_path)
            )
            model3_exists = fs.exists(
                self.spark._jvm.org.apache.hadoop.fs.Path(model3_path)
            )

            if not (model1_exists and model2_exists and model3_exists):
                self.logger.warning("Not all model versions found for latest timestamp")
                return None, None, None

            self.model1 = PipelineModel.load(model1_path)
            self.model2 = PipelineModel.load(model2_path)
            self.model3 = PipelineModel.load(model3_path)
            self.model_version = latest_timestamp

            self.logger.info(f"Successfully loaded models version {latest_timestamp}")
            return self.model1, self.model2, self.model3

        except Exception as e:
            self.logger.error(f"Error loading models: {str(e)}")
            return None, None, None

    def preprocess_dataset1(self, df):
        """Preprocess dataset1"""
        # Rename isFraud to fraud to match other datasets
        return df.withColumn(
            "amount_to_balance_ratio",
            when(
                col("oldbalanceOrg") > 0, col("amount") / col("oldbalanceOrg")
            ).otherwise(0),
        ).withColumnRenamed("isFraud", "fraud")

    def preprocess_dataset2(self, df):
        """Preprocess dataset2"""
        return df

    def preprocess_dataset3(self, df):
        """Preprocess dataset3"""
        return df

    def save_prediction_to_cassandra(self, prediction_data):
        """Save prediction results to Cassandra"""
        try:
            if self.cassandra_session is not None:

                # Generate a unique transaction ID if not present
                transaction_id = prediction_data.get(
                    "transaction_id", str(uuid.uuid4())
                )

                # Calculate ensemble probability (average of all models)
                probabilities = [
                    prediction_data.get("model1_fraud_probability", 0.0),
                    prediction_data.get("model2_fraud_probability", 0.0),
                    prediction_data.get("model3_fraud_probability", 0.0),
                ]
                ensemble_probability = np.mean(probabilities)

                self.cassandra_session.execute(
                    """
                    INSERT INTO real_time_predictions (
                        transaction_id,
                        prediction_timestamp,
                        transaction_type,
                        amount,
                        customer_id,
                        model1_fraud_probability,
                        model2_fraud_probability,
                        model3_fraud_probability,
                        ensemble_fraud_probability,
                        is_fraud,
                        model_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        transaction_id,
                        datetime.now(),
                        prediction_data.get("type", "unknown"),
                        float(prediction_data.get("amount", 0.0)),
                        prediction_data.get("customer_id", "unknown"),
                        float(prediction_data.get("model1_fraud_probability", 0.0)),
                        float(prediction_data.get("model2_fraud_probability", 0.0)),
                        float(prediction_data.get("model3_fraud_probability", 0.0)),
                        float(ensemble_probability),
                        ensemble_probability > 0.5,
                        self.model_version,
                    ),
                )
                self.logger.info(
                    f"Saved prediction for transaction {transaction_id} to Cassandra"
                )
        except Exception as e:
            self.logger.error(f"Error saving prediction to Cassandra: {str(e)}")

    def process_messages(self):
        """Process messages from all Kafka topics"""
        messages = {topic: None for topic in config.kafka_topics}
        try:
            # Get messages from all topics
            for consumer in self.consumers.values():
                message = next(consumer)
                messages[message.topic] = message

            if all(messages.values()):
                message1 = messages["dataset1"].value
                message2 = messages["dataset2"].value
                message3 = messages["dataset3"].value

                # Create DataFrames
                df1 = self.spark.createDataFrame([message1], self.schema1)
                df2 = self.spark.createDataFrame([message2], self.schema2)
                df3 = self.spark.createDataFrame([message3], self.schema3)

                # Preprocess the data
                df1_processed = self.preprocess_dataset1(df1)
                df2_processed = self.preprocess_dataset2(df2)
                df3_processed = self.preprocess_dataset3(df3)

                # Create feature vectors for each model
                test_data1 = self.create_feature_vector1(df1_processed)
                test_data2 = self.create_feature_vector2(df2_processed)
                test_data3 = self.create_feature_vector3(df3_processed)

                # Make predictions
                if self.model1 and self.model2 and self.model3:
                    get_fraud_prob = udf(lambda v: float(v.values[1]), DoubleType())

                    prediction1 = self.model1.transform(test_data1)
                    prediction1 = prediction1.select(
                        "transaction_id",
                        "type",
                        "amount",
                        "nameOrig",
                        "nameDest",
                        "fraud",
                        "prediction",
                        get_fraud_prob("probability").alias("fraud_probability"),
                    )

                    prediction2 = self.model2.transform(test_data2)
                    prediction2 = prediction2.select(
                        "fraud",
                        "prediction",
                        get_fraud_prob("probability").alias("fraud_probability"),
                    )

                    prediction3 = self.model3.transform(test_data3)
                    prediction3 = prediction3.select(
                        "fraud",
                        "prediction",
                        get_fraud_prob("probability").alias("fraud_probability"),
                        "customer_id",
                        "transaction_id",
                    )

                    # Extract prediction results
                    result1 = prediction1.collect()[0]
                    result2 = prediction2.collect()[0]
                    result3 = prediction3.collect()[0]

                    # Calculate ensemble probability (average of all models)
                    ensemble_probability = (
                        float(result1.fraud_probability)
                        + float(result2.fraud_probability)
                        + float(result3.fraud_probability)
                    ) / 3.0

                    # Save prediction to Cassandra
                    prediction_data = {
                        "transaction_id": result1.transaction_id,
                        "type": result1.type,
                        "amount": float(result1.amount),
                        "customer_id": result3.customer_id,
                        "model1_fraud_probability": float(result1.fraud_probability),
                        "model2_fraud_probability": float(result2.fraud_probability),
                        "model3_fraud_probability": float(result3.fraud_probability),
                        "ensemble_fraud_probability": float(ensemble_probability),
                        "prediction_timestamp": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    }

                    self.save_prediction_to_cassandra(prediction_data)

                    self.logger.info(f"Processed transaction: {result1.transaction_id}")
                    return result1
                else:
                    self.logger.warning("Models not trained yet")
                    return None

        except Exception as e:
            self.logger.error(f"Error processing messages: {e}")
            raise

    def run(self):
        """Main processing loop"""
        self.logger.info("Starting Fraud Detection Pipeline...")

        try:
            # Generate training data
            training_df1 = pd.read_csv("./datasets/Fraud.csv")
            training_data1, _ = train_test_split(
                training_df1,
                test_size=0.9999,
                random_state=2025,
                stratify=training_df1["isFraud"],
            )
            training_data1 = training_data1.to_dict(orient="records")
            self.logger.info("Dataset1 is loaded")

            training_data2 = [
                {
                    "distance_from_home": 1500.5,
                    "distance_from_last_transaction": 1489.2,
                    "ratio_to_median_purchase_price": 10.5,
                    "repeat_retailer": 0,
                    "used_chip": 0,
                    "used_pin_number": 0,
                    "online_order": 1,
                    "fraud": 1,
                },
                {
                    "distance_from_home": 3.2,
                    "distance_from_last_transaction": 0.5,
                    "ratio_to_median_purchase_price": 1.2,
                    "repeat_retailer": 1,
                    "used_chip": 1,
                    "used_pin_number": 1,
                    "online_order": 0,
                    "fraud": 0,
                },
                {
                    "distance_from_home": 2500.0,
                    "distance_from_last_transaction": 2489.8,
                    "ratio_to_median_purchase_price": 15.8,
                    "repeat_retailer": 0,
                    "used_chip": 0,
                    "used_pin_number": 0,
                    "online_order": 1,
                    "fraud": 1,
                },
            ]

            training_data3 = [
                {
                    "amt": 9999.99,
                    "bin": 123456,
                    "customer_id": "CUST001",
                    "entry_mode": "ONLINE",
                    "fraud": 1,
                    "fraud_scenario": 1,
                    "post_ts": "2025-01-04T14:30:00Z",
                    "terminal_id": "TERM001",
                    "transaction_id": "TX001",
                },
                {
                    "amt": 250.00,
                    "bin": 789012,
                    "customer_id": "CUST002",
                    "entry_mode": "CHIP",
                    "fraud": 0,
                    "fraud_scenario": 0,
                    "post_ts": "2025-01-04T14:35:00Z",
                    "terminal_id": "TERM002",
                    "transaction_id": "TX002",
                },
                {
                    "amt": 15000.00,
                    "bin": 345678,
                    "customer_id": "CUST003",
                    "entry_mode": "ONLINE",
                    "fraud": 1,
                    "fraud_scenario": 2,
                    "post_ts": "2025-01-04T14:40:00Z",
                    "terminal_id": "TERM003",
                    "transaction_id": "TX003",
                },
            ]

            self.train_models(training_data1, training_data2, training_data3)

            while True:
                self.process_messages()

        except KeyboardInterrupt:
            self.logger.info("Shutting down pipeline...")
        except Exception as e:
            self.logger.error(f"Fatal error in pipeline: {e}")
            raise
        finally:
            for consumer in self.consumers.values():
                consumer.close()
            if self.spark:
                self.spark.stop()


def preprocess_row_1(row):
    """
    Data preprocessing for a single row in dataset 1 ('Fraudulent Transactions Data' from Kaggle)

    Parameters:
      raw (dict): A single unprocessed row in the dataset

    Returns:
      dict: Preprocessed row with keys: 'type', 'amount', 'oldbalanceOrg', 'newbalanceOrig',
        'isMerchant', 'isFlaggedFraud', 'isFraud' transformed to float/int values only.
    """
    transaction_types = {
        "CASH-IN": 1,
        "CASH-OUT": 2,
        "DEBIT": 3,
        "PAYMENT": 4,
        "TRANSFER": 5,
    }
    return {
        "type": transaction_types.get(row["type"], 0),
        "amount": row["amount"],
        "oldbalanceOrg": row["oldbalanceOrg"],
        "newbalanceOrig": row["newbalanceOrig"],
        "isMerchant": row["nameDest"].startswith("M"),
        "isFlaggedFraud": row["isFlaggedFraud"],
        "isFraud": row["isFraud"],
    }


def preprocess_row_2(row):
    return {
        "distance_from_home": row["distance_from_home"],
        "distance_from_last_transaction": row["distance_from_last_transaction"],
        "ratio_to_median_purchase_price": row["ratio_to_median_purchase_price"],
        "repeat_retailer": int(row["repeat_retailer"]),
        "used_chip": int(row["used_chip"]),
        "used_pin_number": int(row["used_pin_number"]),
        "isFraud": int(row["fraud"]),
    }


def preprocess_row_3(row):
    entry_modes = {"Contactless": 1, "Chip": 2, "Swipe": 3}
    return {
        "customer_id": row["customer_id"],
        "bin": row["bin"],
        "amount": row["amt"],
        "entry_mode": entry_modes.get(row["entry_mode"], 0),
        "isFraud": 0,
    }


def preprocess_row_4(row):
    result = row.copy()
    result["amount"] = row["Amount"]
    result["isFraud"] = row["Class"]

    # cleanup
    del result["Time"]
    del result["Amount"]
    del result["Class"]
    return result
