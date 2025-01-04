import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum, col, when, desc, hour, dayofweek, month, year
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a Spark session with Hive support"""
    return SparkSession.builder \
        .appName("Batch Processing") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

def create_cassandra_session():
    """Create a connection to Cassandra"""
    try:
        cluster = Cluster(['cassandra'], auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
        session = cluster.connect()
        session.set_keyspace('fraud_analytics')
        return session
    except Exception as e:
        logger.error(f"Error connecting to Cassandra: {str(e)}")
        return None

def query_hive(spark, table_name):
    """Query data from Hive table"""
    try:
        return spark.sql(f"SELECT * FROM fraud.{table_name}")
    except Exception as e:
        logger.error(f"Error querying table {table_name}: {str(e)}")
        return None

def create_and_save_views(spark, cassandra_session):
    """Create views and save them to Cassandra"""
    try:
        # Get data from Hive
        df1 = query_hive(spark, "dataset1")
        df3 = query_hive(spark, "dataset3")  # Contains customer data
        
        if df1 is not None:
            # View 1: Fraud statistics by transaction type
            fraud_by_type = df1.groupBy("type").agg(
                count("*").alias("total_transactions"),
                sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("total_fraudulent"),
                avg("amount").alias("avg_amount")
            ).withColumn(
                "fraud_rate",
                col("total_fraudulent") / col("total_transactions")
            )
            
            # Save to Cassandra
            for row in fraud_by_type.collect():
                cassandra_session.execute(
                    """
                    INSERT INTO fraud_by_transaction_type 
                    (type, total_transactions, total_fraudulent, avg_amount, fraud_rate)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (row.type, row.total_transactions, row.total_fraudulent, 
                     float(row.avg_amount), float(row.fraud_rate))
                )
        
        if df3 is not None:
            # View 2: Hourly fraud statistics
            hourly_stats = df3.groupBy(hour("post_ts").alias("hour")).agg(
                count("*").alias("total_transactions"),
                sum(when(col("fraud") == 1, 1).otherwise(0)).alias("total_fraudulent"),
                avg("amt").alias("avg_amount")
            )
            
            # Save to Cassandra
            for row in hourly_stats.collect():
                cassandra_session.execute(
                    """
                    INSERT INTO hourly_fraud_stats 
                    (hour, total_transactions, total_fraudulent, avg_amount)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (row.hour, row.total_transactions, row.total_fraudulent, float(row.avg_amount))
                )
            
            # View 3: High-risk customer analysis
            customer_stats = df3.groupBy("customer_id").agg(
                count("*").alias("total_transactions"),
                sum(when(col("fraud") == 1, 1).otherwise(0)).alias("fraudulent_transactions"),
                sum("amt").alias("total_amount")
            ).withColumn(
                "fraud_rate",
                col("fraudulent_transactions") / col("total_transactions")
            ).filter(col("total_transactions") >= 5)  # Only customers with sufficient history
            
            # Save to Cassandra
            for row in customer_stats.collect():
                cassandra_session.execute(
                    """
                    INSERT INTO high_risk_customers 
                    (customer_id, total_transactions, fraudulent_transactions, total_amount, fraud_rate)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (row.customer_id, row.total_transactions, row.fraudulent_transactions,
                     float(row.total_amount), float(row.fraud_rate))
                )
                
    except Exception as e:
        logger.error(f"Error creating views: {str(e)}")

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()
    
    while True:
        try:
            # Create Cassandra session
            cassandra_session = create_cassandra_session()
            if cassandra_session is None:
                raise Exception("Failed to connect to Cassandra")
            
            # Create and save views
            create_and_save_views(spark, cassandra_session)
            logger.info("Successfully updated views in Cassandra")
            
            # Close Cassandra connection
            cassandra_session.shutdown()
            
            # Sleep for 5 minutes before next batch
            time.sleep(300)
            
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            time.sleep(60)  # Wait a minute before retrying on error
