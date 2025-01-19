import mlflow
import mlflow.spark
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
# import uuid
# import os

# Initialize Spark session
spark = SparkSession.builder.appName("MLflowPySparkDataset1").getOrCreate()

# def create_feature_vector1(df):
#     """Create feature vector for model1 (dataset1)"""
#     try:
#         df = df.withColumn(
#             "amount_to_balance_ratio",
#             when(
#                 col("oldbalanceOrg") > 0, col("amount") / col("oldbalanceOrg")
#             ).otherwise(0),
#         ).withColumnRenamed("isFraud", "fraud")
#         # Add transaction_id column if it doesn't exist
#         df = df.withColumn(
#             "transaction_id",
#             when(
#                 col("nameOrig").isNotNull(),
#                 concat(col("nameOrig"), lit("_"), col("step").cast("string")),
#             ).otherwise(lit(str(uuid.uuid4()))),
#         )
#         df = df.withColumn("amount", col("amount").cast("float"))
#         df = df.withColumn("fraud", col("fraud").cast("int"))

#         feature_cols = ["amount", "amount_to_balance_ratio"]

#         assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

#         # Select all necessary columns including those needed for predictions
#         return assembler.transform(df).select(
#             "features",
#             "fraud",
#             "transaction_id",
#             "type",
#             "amount",
#             "nameOrig",
#             "nameDest",
#         )
#     except Exception as e:
#         print(f"Error creating feature vector for model1: {str(e)}")
#         raise

# Sample Data (for illustration purposes, this should be a larger dataset in practice)
data = spark.createDataFrame([
    (0, 1.0, 1.0),
    (1, 1.0, 2.0),
    (2, 2.0, 1.0),
    (3, 2.0, 2.0),
    (4, 3.0, 3.0),
    (5, 3.0, 4.0),
], ["label", "feature1", "feature2"])

# Create feature vector
assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
data = assembler.transform(data)

# Train the RandomForestClassifier
rf = RandomForestClassifier(labelCol="label", featuresCol="features")
pipeline = Pipeline(stages=[assembler, rf])

# Fit the model
model = pipeline.fit(data)

# Log the model with MLflow
with mlflow.start_run():
    mlflow.spark.log_model(model, "random_forest_model")
    mlflow.log_metric("num_trees", 100)  # Example metric, change based on your model
    print("Model logged to MLflow.")