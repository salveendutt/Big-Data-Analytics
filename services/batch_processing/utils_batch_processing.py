import time
import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from cassandra.auth import PlainTextAuthProvider
from config_batch_processing import (
    cassandra_connection_attempts,
    cassandra_connection_attempts_delay,
    cassandra_ip,
    cassandra_keyspace,
    cassandra_password,
    cassandra_port,
    cassandra_username,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a Spark session with Hive support"""
    return (
        SparkSession.builder.appName("Batch Processing")
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .enableHiveSupport()
        .getOrCreate()
    )


def create_cassandra_session():
    """Create a connection to Cassandra"""
    try:
        cluster = Cluster(
            ["cassandra"],
            auth_provider=PlainTextAuthProvider(
                username="cassandra", password="cassandra"
            ),
        )
        session = cluster.connect()
        session.set_keyspace("fraud_analytics")
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

def save_to_cassandra(df, cassandra_session, table_name: str, columns: tuple):
    """Save DataFrame to Cassandra table."""
    try:
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        
        for row in df.collect():
            values = tuple(float(row[col]) if isinstance(row[col], (int, float)) 
                         else row[col] for col in columns)
            cassandra_session.execute(query, values)
    except Exception as e:
        logger.error(f"Error saving to {table_name}: {str(e)}")
        raise


# def create_cassandra_session():
#     attempts = 0
#     while attempts < cassandra_connection_attempts:
#         try:
#             auth_provider = PlainTextAuthProvider(
#                 username=cassandra_username, password=cassandra_password
#             )
#             cluster = Cluster(
#                 [cassandra_ip], port=cassandra_port, auth_provider=auth_provider
#             )
#             session = cluster.connect(cassandra_keyspace)
#             return session
#         except Exception as e:
#             print(f"Attempt {attempts + 1} failed: {e}")
#             attempts += 1
#             time.sleep(cassandra_connection_attempts_delay)
#     else:
#         print(
#             f"Failed to connect to Cassandra after {cassandra_connection_attempts} attempts"
#         )
#         exit(1)
#     return None


# def query_cassandra(session, query):
#     rows = session.execute(query)
#     return rows

