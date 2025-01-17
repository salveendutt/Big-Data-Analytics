from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    from_json,
    lit,
    hour,
    minute,
    to_timestamp,
    concat,
    count,
    avg,
    sum,
    udf,
    expr,
)
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
import uuid
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"


def create_feature_vector1(df):
    """Create feature vector for model1 (dataset1)"""
    try:
        df = df.withColumn(
            "amount_to_balance_ratio",
            when(
                col("oldbalanceOrg") > 0, col("amount") / col("oldbalanceOrg")
            ).otherwise(0),
        ).withColumnRenamed("isFraud", "fraud")
        # Add transaction_id column if it doesn't exist
        df = df.withColumn(
            "transaction_id",
            when(
                col("nameOrig").isNotNull(),
                concat(col("nameOrig"), lit("_"), col("step").cast("string")),
            ).otherwise(lit(str(uuid.uuid4()))),
        )
        df = df.withColumn("amount", col("amount").cast("float"))
        df = df.withColumn("fraud", col("fraud").cast("int"))

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
        print(f"Error creating feature vector for model1: {str(e)}")
        raise


def create_feature_vector2(df):
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
        df = df.select(
            col("distance_from_home").cast("float").alias("distance_from_home"),
            col("distance_from_last_transaction")
            .cast("float")
            .alias("distance_from_last_transaction"),
            col("fraud").cast("int").alias("fraud"),
            col("ratio_to_median_purchase_price")
            .cast("float")
            .alias("ratio_to_median_purchase_price"),
            col("repeat_retailer").cast("int").alias("repeat_retailer"),
            col("used_chip").cast("int").alias("used_chip"),
            col("used_pin_number").cast("int").alias("used_pin_number"),
            col("online_order").cast("int").alias("online_order"),
        )

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        return assembler.transform(df).select("features", "fraud")
    except Exception as e:
        print(f"Error creating feature vector for model2: {str(e)}")
        raise


def create_feature_vector3(df):
    """Create feature vector for model3 (dataset3)"""
    try:
        feature_cols = ["amt"]

        df = df.select(
            col("amt").cast("float").alias("amt"),
            col("fraud").cast("int").alias("fraud"),
            col("customer_id").alias("customer_id"),
            col("transaction_id").alias("transaction_id"),
        )
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        return assembler.transform(df).select(
            "features", "fraud", "customer_id", "transaction_id"
        )
    except Exception as e:
        print(f"Error creating feature vector for model3: {str(e)}")
        raise


spark = (
    SparkSession.builder.appName("Local Training")
    .master("local[2]")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

try:
    fraud_data = spark.read.csv("../datasets/test_Fraud.csv", header=True)
    credit_card_data = spark.read.csv(
        "../datasets/test_Credit_Card_Fraud_.csv", header=True
    )
    transactions_data = spark.read.csv(
        "../datasets/test_transactions_df.csv", header=True
    )

    fraud_data = fraud_data.withColumn(
        "amount_to_balance_ratio",
        when(col("oldbalanceOrg") > 0, col("amount") / col("oldbalanceOrg")).otherwise(
            0
        ),
    )
    fraud_data = create_feature_vector1(fraud_data)
    credit_card_data = create_feature_vector2(credit_card_data)
    transactions_data = create_feature_vector3(transactions_data)

    # Initialize models
    rf1 = RandomForestClassifier(labelCol="fraud", featuresCol="features", numTrees=10)
    rf2 = RandomForestClassifier(labelCol="fraud", featuresCol="features", numTrees=10)
    rf3 = RandomForestClassifier(labelCol="fraud", featuresCol="features", numTrees=10)

    # Create pipelines
    pipeline1 = Pipeline(stages=[rf1])
    pipeline2 = Pipeline(stages=[rf2])
    pipeline3 = Pipeline(stages=[rf3])

    # Train models
    model1 = pipeline1.fit(fraud_data)
    model2 = pipeline2.fit(credit_card_data)
    model3 = pipeline3.fit(transactions_data)
    current_path = os.path.dirname(os.path.abspath(__file__))

    model1.write().overwrite().save(os.path.join(current_path, "models/rf_fraud_model"))
    model2.write().overwrite().save(
        os.path.join(current_path, "models/rf_credit_card_model")
    )
    model3.write().overwrite().save(
        os.path.join(current_path, "models/rf_transactions_model")
    )
    import shutil

    shutil.move(
        "./models/rf_fraud_model",
        "../services/streaming_processing/models/rf_fraud_model",
    )
    shutil.move(
        "./models/rf_credit_card_model",
        "../services/streaming_processing/models/rf_credit_card_model",
    )
    shutil.move(
        "./models/rf_transactions_model",
        "../services/streaming_processing/models/rf_transactions_model",
    )
except Exception as e:
    print(f"Error training models: {e}")
    spark.stop()
finally:
    spark.stop()
