import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    avg,
    sum,
    col,
    when,
    desc,
    hour,
    dayofweek,
    month,
    year,
)
from utils_batch_processing import (
    create_spark_session,
    create_cassandra_session,
    query_hive,
    save_to_cassandra,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Defining all the views
def create_fraud_by_type_view(df):
    """Calculate fraud statistics by transaction type."""
    return (
        df.groupBy("type")
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("total_fraudulent"),
            avg("amount").alias("avg_amount"),
        )
        .withColumn("fraud_rate", col("total_fraudulent") / col("total_transactions"))
    )


def create_fraud_by_amount_view(df):
    """Calculate fraud statistics by amount ranges."""
    return (
        df.withColumn(
            "amount_bucket",
            when(col("amount") <= 1000, "0-1000")
            .when(col("amount") <= 10000, "1000-10000")
            .when(col("amount") <= 50000, "10000-50000")
            .when(col("amount") <= 100000, "50000-100000")
            .when(col("amount") <= 500000, "100000-500000")
            .otherwise("500000+"),
        )
        .groupBy("amount_bucket")
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("isFraud") == 1, 1).otherwise(0)).alias("total_fraudulent"),
            avg("amount").alias("avg_amount"),
        )
        .withColumn("fraud_rate", col("total_fraudulent") / col("total_transactions"))
    )


def create_hourly_stats_view(df):
    """Calculate hourly fraud statistics."""
    return df.groupBy(hour("post_ts").alias("hour")).agg(
        count("*").alias("total_transactions"),
        sum(when(col("fraud") == 1, 1).otherwise(0)).alias("total_fraudulent"),
        avg("amt").alias("avg_amount"),
    )


def create_customer_risk_view(df):
    """Analyze high-risk customers."""
    return (
        df.groupBy("customer_id")
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("fraud") == 1, 1).otherwise(0)).alias(
                "fraudulent_transactions"
            ),
            sum("amt").alias("total_amount"),
        )
        .withColumn(
            "fraud_rate", col("fraudulent_transactions") / col("total_transactions")
        )
        .filter(col("total_transactions") >= 5)
    )


def create_and_save_views(spark: SparkSession, cassandra_session):
    """Create views and save them to Cassandra."""
    try:
        # Process Dataset 1
        df1 = query_hive(spark, "dataset1")
        if df1 is not None:
            # Fraud by type view
            fraud_by_type = create_fraud_by_type_view(df1)
            save_to_cassandra(
                fraud_by_type,
                cassandra_session,
                "fraud_by_transaction_type",
                (
                    "type",
                    "total_transactions",
                    "total_fraudulent",
                    "avg_amount",
                    "fraud_rate",
                ),
            )

            # Fraud by amount view
            fraud_by_amount = create_fraud_by_amount_view(df1)
            save_to_cassandra(
                fraud_by_amount,
                cassandra_session,
                "fraud_by_amount_bucket",
                (
                    "amount_bucket",
                    "total_transactions",
                    "total_fraudulent",
                    "avg_amount",
                    "fraud_rate",
                ),
            )

        # Process Dataset 3
        df3 = query_hive(spark, "dataset3")
        if df3 is not None:
            # Hourly stats view
            hourly_stats = create_hourly_stats_view(df3)
            save_to_cassandra(
                hourly_stats,
                cassandra_session,
                "hourly_fraud_stats",
                ("hour", "total_transactions", "total_fraudulent", "avg_amount"),
            )

            # Customer risk view
            customer_stats = create_customer_risk_view(df3)
            save_to_cassandra(
                customer_stats,
                cassandra_session,
                "high_risk_customers",
                (
                    "customer_id",
                    "total_transactions",
                    "fraudulent_transactions",
                    "total_amount",
                    "fraud_rate",
                ),
            )

    except Exception as e:
        logger.error(f"Error creating views: {str(e)}")


if __name__ == "__main__":
    spark = create_spark_session()

    while True:
        try:
            cassandra_session = create_cassandra_session()
            if cassandra_session is None:
                raise Exception("Failed to connect to Cassandra")

            create_and_save_views(spark, cassandra_session)
            logger.info("Successfully updated views in Cassandra")

            cassandra_session.shutdown()

            time.sleep(300)

        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            time.sleep(60)
