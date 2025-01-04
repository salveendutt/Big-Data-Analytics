import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, from_json, lit, hour, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.ml import Pipeline
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
import numpy
import uuid


class FraudDetectionPipeline:
    def __init__(self):
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        # Convert timestamp string to datetime object
        self.timestamp_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        # Initialize Spark session
        self.spark = self._initialize_spark()
        
        # Initialize schemas
        self._initialize_schemas()
        
        # Initialize Kafka connections
        self.consumers = self._initialize_kafka_consumers()
        self.producer = self._initialize_kafka_producer()
        
        # Initialize Cassandra connection
        self.cassandra_session = self._initialize_cassandra()
        
        # Try to load existing model or set to None
        self.model = self.load_latest_model_from_hdfs()
        self.model_version = "latest" if self.model else "1.0.0"

    def _initialize_spark(self):
        """Initialize Spark session with retry logic"""
        try:
            return SparkSession.builder \
                .master(config.spark_master_address) \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
                .config("spark.jars.packages", config.spark_jars_packages) \
                .appName("Fraud Detection Pipeline") \
                .getOrCreate()

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
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                        group_id=f'fraud-detection-group-{topic}',
                        auto_offset_reset='latest'
                    )
                self.logger.info("Successfully connected to Kafka consumers")
                return consumers
            except NoBrokersAvailable:
                if attempt < config.kafka_connection_attempts - 1:
                    self.logger.warning(f"Failed to connect to Kafka, attempt {attempt + 1} of {config.kafka_connection_attempts}")
                    time.sleep(config.kafka_connection_attempts_delay)
                else:
                    self.logger.error("Failed to connect to Kafka after all attempts")
                    raise

    def _initialize_kafka_producer(self):
        """Initialize Kafka producer with retry logic"""
        for attempt in range(config.kafka_connection_attempts):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=config.kafka_address,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                self.logger.info("Successfully connected to Kafka producer")
                return producer
            except NoBrokersAvailable:
                if attempt < config.kafka_connection_attempts - 1:
                    self.logger.warning(f"Failed to connect to Kafka producer, attempt {attempt + 1} of {config.kafka_connection_attempts}")
                    time.sleep(config.kafka_connection_attempts_delay)
                else:
                    self.logger.error("Failed to connect to Kafka producer after all attempts")
                    raise

    def _initialize_cassandra(self):
        """Initialize connection to Cassandra"""
        try:
            cluster = Cluster(['cassandra'], auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
            session = cluster.connect()
            session.set_keyspace('fraud_analytics')
            return session
        except Exception as e:
            self.logger.error(f"Error connecting to Cassandra: {str(e)}")
            return None

    def _initialize_schemas(self):
        """Initialize schemas for each dataset"""
        self.schema1 = StructType([
            StructField("step", IntegerType()),
            StructField("type", StringType()),
            StructField("amount", DoubleType()),
            StructField("nameOrig", StringType()),
            StructField("oldbalanceOrg", DoubleType()),
            StructField("newbalanceOrig", DoubleType()),
            StructField("nameDest", StringType()),
            StructField("oldbalanceDest", DoubleType()),
            StructField("newbalanceDest", DoubleType()),
            StructField("isFraud", IntegerType()),
            StructField("isFlaggedFraud", IntegerType())
        ])

        self.schema2 = StructType([
            StructField("distance_from_home", DoubleType()),
            StructField("distance_from_last_transaction", DoubleType()),
            StructField("fraud", IntegerType()),
            StructField("ratio_to_median_purchase_price", DoubleType()),
            StructField("repeat_retailer", IntegerType()),
            StructField("used_chip", IntegerType()),
            StructField("used_pin_number", IntegerType()),
            StructField("online_order", IntegerType())
        ])

        self.schema3 = StructType([
            StructField("amt", DoubleType()),
            StructField("bin", IntegerType()),
            StructField("customer_id", StringType()),
            StructField("entry_mode", StringType()),
            StructField("fraud", IntegerType()),
            StructField("fraud_scenario", IntegerType()),
            StructField("post_ts", StringType()),
            StructField("terminal_id", StringType()),
            StructField("transaction_id", StringType())
        ])

    def save_model_to_hdfs(self, model):
        """Save the trained model to HDFS"""
        try:
            # Generate model path with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            model_path = f"/user/models/fraud_detection/model_{timestamp}"
            
            # Save model to HDFS
            model.save(model_path)
            
            # Update model version to match the saved path
            self.model_version = f"model_{timestamp}"
            self.logger.info(f"Successfully saved model to HDFS: {model_path}")
            
            return model_path
        except Exception as e:
            self.logger.error(f"Error saving model to HDFS: {str(e)}")
            raise

    def load_latest_model_from_hdfs(self):
        """Load the latest model from HDFS"""
        try:
            # List all models in the directory
            models_path = "/user/models/fraud_detection"
            models = self.spark._jvm.org.apache.hadoop.fs.Path(models_path)
            fs = models.getFileSystem(self.spark._jsc.hadoopConfiguration())
            
            if not fs.exists(models):
                self.logger.info("No existing models found in HDFS")
                return None
            
            # Get all model directories
            status = fs.listStatus(models)
            if not status:
                return None
            
            # Find the latest model
            latest_model = max(
                [path.getPath().toString() for path in status],
                key=lambda x: x.split('_')[-1]
            )
            
            # Load the model
            from pyspark.ml import PipelineModel
            model = PipelineModel.load(latest_model)
            self.model_version = latest_model.split('/')[-1]
            self.logger.info(f"Successfully loaded model from HDFS: {latest_model}")
            
            return model
        except Exception as e:
            self.logger.error(f"Error loading model from HDFS: {str(e)}")
            return None

    def send_processed_message(self, result):
        """Send processed result to output topic"""
        try:
            self.producer.send(
                config.kafka_topic_processed,
                {
                    'transaction_id': result.transaction_id,
                    'prediction': float(result.prediction),
                    'probability': float(result.probability[1]),  # Probability of fraud
                    'timestamp': datetime.now().isoformat(),
                    'amount': float(result.amount)
                }
            )
            self.producer.flush()
            self.logger.info(f"Sent prediction for transaction {result.transaction_id}")
        except Exception as e:
            self.logger.error(f"Error sending processed message: {e}")

    def preprocess_dataset1(self, df):
        """Preprocess dataset1 specific features"""
        try:
            type_indexer = StringIndexer(inputCol="type", outputCol="type_index")
            
            df = df.withColumn("amount_to_balance_ratio", 
                              when(col("oldbalanceOrg") > 0, 
                                   col("amount") / col("oldbalanceOrg")).otherwise(0))
            
            return df, [type_indexer]
        except Exception as e:
            self.logger.error(f"Error preprocessing dataset1: {e}")
            raise

    def preprocess_dataset2(self, df):
        """Preprocess dataset2 specific features"""
        return df, []

    def preprocess_dataset3(self, df):
        """Preprocess dataset3 specific features"""
        try:
            entry_mode_indexer = StringIndexer(inputCol="entry_mode", outputCol="entry_mode_index")
            df = df.withColumn("post_ts", to_timestamp(col("post_ts"), self.timestamp_format))
            df = df.withColumn("hour", hour(col("post_ts")))
            return df, [entry_mode_indexer]
        except Exception as e:
            self.logger.error(f"Error preprocessing dataset3: {e}")
            raise

    def create_feature_vector(self, df1, df2, df3):
        """Combine features from all datasets and create final feature vector"""
        features1 = ["amount", "amount_to_balance_ratio", "type_index"]
        features2 = ["distance_from_home", "distance_from_last_transaction", 
                    "ratio_to_median_purchase_price", "used_chip", "used_pin_number"]
        features3 = ["amt", "entry_mode_index", "hour"]
        
        assembler = VectorAssembler(
            inputCols=features1 + features2 + features3,
            outputCol="features",
            handleInvalid="keep"
        )
        
        return assembler

    def train_model(self, training_data1: List[Dict], training_data2: List[Dict], 
                   training_data3: List[Dict]):
        """Train the Random Forest model"""
        try:
            # Convert training data to DataFrames
            train_data1_modified = [{**d, 'fraud': d.get('isFraud')} for d in training_data1]
            train_df1 = self.spark.createDataFrame(train_data1_modified, self.schema1)
            train_df2 = self.spark.createDataFrame(training_data2, self.schema2)
            train_df3 = self.spark.createDataFrame(training_data3, self.schema3)
            
            # Preprocess each dataset
            train_df1, transformers1 = self.preprocess_dataset1(train_df1)
            train_df2, transformers2 = self.preprocess_dataset2(train_df2)
            train_df3, transformers3 = self.preprocess_dataset3(train_df3)
            
            # Create feature vector
            assembler = self.create_feature_vector(train_df1, train_df2, train_df3)
            
            # Initialize Random Forest Classifier
            rf = RandomForestClassifier(
                labelCol="fraud",
                featuresCol="features",
                numTrees=100,
                maxDepth=10,
                seed=42
            )
            
            # Create and train pipeline
            pipeline = Pipeline(stages=transformers1 + transformers2 + transformers3 + [assembler, rf])
            train_df = train_df1.join(train_df2, ["fraud"]).join(train_df3, ["fraud"])
            
            if (train_df.count() < 2):
                self.logger.error("Not enough data for training")
                return
            self.model = pipeline.fit(train_df)
            
            # Save model to HDFS
            model_path = self.save_model_to_hdfs(self.model)
            self.logger.info(f"Model training completed and saved to {model_path}")
            
            # Calculate training metrics
            predictions = self.model.transform(
                train_df1.join(train_df2, ["fraud"]).join(train_df3, ["fraud"])
            )
            evaluator = BinaryClassificationEvaluator(labelCol="fraud")
            auc_roc = evaluator.evaluate(predictions)
            self.logger.info(f"Training AUC-ROC: {auc_roc}")
            
        except Exception as e:
            self.logger.error(f"Error training model: {e}")
            raise

    def save_prediction_to_cassandra(self, prediction_data):
        """Save prediction results to Cassandra"""
        try:
            if self.cassandra_session is not None:
                # Generate a unique transaction ID if not present
                transaction_id = prediction_data.get('transaction_id', str(uuid.uuid4()))
                
                self.cassandra_session.execute("""
                    INSERT INTO real_time_predictions (
                        transaction_id,
                        prediction_timestamp,
                        transaction_type,
                        amount,
                        customer_id,
                        fraud_probability,
                        is_fraud,
                        model_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    transaction_id,
                    datetime.now(),
                    prediction_data.get('type', 'unknown'),
                    float(prediction_data.get('amount', 0.0)),
                    prediction_data.get('customer_id', 'unknown'),
                    float(prediction_data.get('fraud_probability', 0.0)),
                    prediction_data.get('fraud_probability', 0.0) > 0.5,
                    self.model_version
                ))
                self.logger.info(f"Saved prediction for transaction {transaction_id} to Cassandra")
        except Exception as e:
            self.logger.error(f"Error saving prediction to Cassandra: {str(e)}")

    def process_messages(self):
        """Process messages from all Kafka topics"""
        messages = {topic: None for topic in config.kafka_topics}
        
        try:
            for topic, consumer in self.consumers.items():
                messages[topic] = next(consumer)
            
            if all(messages.values()):
                # Modify dataset1 message to use 'fraud' instead of 'isFraud'
                message1 = messages['dataset1'].value
                message1['fraud'] = message1.pop('isFraud') if 'isFraud' in message1 else 0
                
                # Convert messages to DataFrames
                df1 = self.spark.createDataFrame([message1], self.schema1)
                df2 = self.spark.createDataFrame([messages['dataset2'].value], self.schema2)
                df3 = self.spark.createDataFrame([messages['dataset3'].value], self.schema3)
                
                # Make prediction
                if self.model:
                    # Preprocess and combine data
                    combined_df = df1.join(df2, ["fraud"]).join(df3, ["fraud"])
                    prediction = self.model.transform(combined_df)
                    
                    # Extract prediction result
                    result = prediction.select(
                        "transaction_id", "amount", "fraud", "prediction", 
                        "probability"
                    ).collect()[0]
                    
                    # Save prediction to Cassandra
                    prediction_data = {
                        'transaction_id': result.transaction_id,
                        'type': result.type,
                        'amount': result.amount,
                        'customer_id': result.customer_id,
                        'fraud_probability': result.probability[1]
                    }
                    self.save_prediction_to_cassandra(prediction_data)
                    
                    # Send processed result to output topic
                    self.send_processed_message(result)
                    
                    self.logger.info(f"Processed transaction: {result.transaction_id}")
                    return result
                else:
                    self.logger.warning("Model not trained yet")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error processing messages: {e}")
            return None

    def run(self):
        """Main processing loop"""
        self.logger.info("Starting Fraud Detection Pipeline...")
        
        try:
            # Example training data (you would normally load this from a file or database)
            training_data1 = [
                {
                    "step": 1,
                    "type": "TRANSFER",
                    "amount": 5000.00,
                    "isFlaggedFraud": 0,
                    "isFraud": 1,
                    "nameDest": "C12345678",
                    "nameOrig": "A98765432",
                    "newbalanceDest": 10000.00,
                    "newbalanceOrig": 0.00,
                    "oldbalanceDest": 5000.00,
                    "oldbalanceOrg": 5000.00
                },
                # Add more training examples
            ]
            
            training_data2 = [
                {
                    "distance_from_home": 2.5,
                    "distance_from_last_transaction": 1.2,
                    "fraud": 0,
                    "online_order": 1,
                    "ratio_to_median_purchase_price": 0.8,
                    "repeat_retailer": 1,
                    "used_chip": 1,
                    "used_pin_number": 1
                },
                # Add more training examples
            ]
            
            training_data3 = [
                {
                    "amt": 120.50,
                    "bin": "123456",
                    "customer_id": "CUST0001",
                    "entry_mode": "CHIP",
                    "fraud": 0,
                    "fraud_scenario": 0,
                    "post_ts": "2024-12-07T12:00:00Z",
                    "terminal_id": "TERM001",
                    "transaction_id": "TXN0001"
                },
                # Add more training examples
            ]
            
            # Train the model
            self.train_model(training_data1, training_data2, training_data3)
            
            # Process streaming data
            while True:
                self.process_messages()
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down pipeline...")
        except Exception as e:
            self.logger.error(f"Fatal error in pipeline: {e}")
            raise
        finally:
            # Clean up resources
            for consumer in self.consumers.values():
                consumer.close()
            if self.producer:
                self.producer.close()
            if self.spark:
                self.spark.stop()
if __name__ == "__main__":
    pipeline = FraudDetectionPipeline()
    pipeline.run()