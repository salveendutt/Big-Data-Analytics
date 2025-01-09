import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from cassandra.auth import PlainTextAuthProvider
from config_batch_processing import (
    cassandra_keyspace,
    cassandra_password,
    cassandra_username,
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
                username=cassandra_username, password=cassandra_password
            ),
        )
        session = cluster.connect()
        session.set_keyspace(cassandra_keyspace)
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
        cassandra_session.execute(f"TRUNCATE {table_name}")

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

        for row in df.collect():
            values = tuple(
                float(row[col]) if isinstance(row[col], (float)) else row[col]
                for col in columns
            )
            cassandra_session.execute(query, values)
    except Exception as e:
        logger.error(f"Error saving to {table_name}: {str(e)}")
        raise
