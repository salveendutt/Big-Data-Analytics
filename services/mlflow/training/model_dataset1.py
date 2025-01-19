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
import os

print("KDBG START model retraining for dataset 1")

# os.environ["MLFLOW_TRACKING_URI"] = "file:/mlflow"
# os.environ["BACKEND_STORE_URI"] = "sqlite:///mlflow/mlflow.db"
# os.environ["ARTIFACT_ROOT"] = "/mlflow/artifacts"

# os.environ["MLFLOW_RUNS_DIR"]= "/mlflow"
# os.environ["BACKEND_STORE_DB_PATH"]= "/mlflow/mlflow.db"
# os.environ["BACKEND_STORE_URI"]= "sqlite:///mlflow/mlflow.db"
# os.environ["DEFAULT_ARTIFACT_ROOT"]= "/mlflow/artifacts"
# ENV BACKEND_STORE_DB_PATH=$MLFLOW_RUNS_DIR/mlflow.db
# ENV BACKEND_STORE_URI=sqlite:///$BACKEND_STORE_DB_PATH
# ENV DEFAULT_ARTIFACT_ROOT=$MLFLOW_RUNS_DIR/artifacts

os.environ["MLFLOW_TRACKING_URI"] = "file:///mlflow"
os.environ["BACKEND_STORE_URI"] = "sqlite:///mlflow\\mlflow.db"
os.environ["ARTIFACT_ROOT"] = "/mlflow/artifacts"

# Define experiment name
experiment_name = "RandomForestDataset1"

# Try to get the experiment by name
experiment = mlflow.get_experiment_by_name(experiment_name)

# If the experiment does not exist, create it
if experiment is None:
    experiment_id = mlflow.create_experiment(experiment_name)
    print(f"Created experiment: {experiment_name}")
else:
    experiment_id = experiment.experiment_id
    print(f"Experiment already exists: {experiment_name}")

# Set the experiment
mlflow.set_experiment(experiment_name)

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
pipeline = Pipeline(stages=[rf])  # Remove assembler from the pipeline

# Fit the model
model = pipeline.fit(data)

# Log the model with MLflow
with mlflow.start_run():
    print("KDBG start run of mlflow")
    # Log the trained model to MLflow
    mlflow.spark.log_model(model, "random_forest_model")
    # Log the number of trees in the model
    num_trees = model.stages[0].getNumTrees
    mlflow.log_metric("num_trees", num_trees)
    print("Model logged to MLflow.")