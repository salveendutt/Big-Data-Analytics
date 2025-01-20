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
import uuid
import os

os.environ["GIT_PYTHON_REFRESH"] = "quiet"

print(f"ARTIFACT_ROOT={os.environ["ARTIFACT_ROOT"]}")

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

spark = (
    SparkSession.builder.appName("MLflowPySparkDataset1")
    .getOrCreate()
)

try:
    fraud_data = spark.read.csv("/app/train_Fraud.csv", header=True)
    # credit_card_data = spark.read.csv(
    #     "../datasets/test_Credit_Card_Fraud_.csv", header=True
    # )
    # transactions_data = spark.read.csv(
    #     "../datasets/test_transactions_df.csv", header=True
    # )

    fraud_data = fraud_data.withColumn(
        "amount_to_balance_ratio",
        when(col("oldbalanceOrg") > 0, col("amount") / col("oldbalanceOrg")).otherwise(
            0
        ),
    )
    fraud_data = create_feature_vector1(fraud_data)
    # credit_card_data = create_feature_vector2(credit_card_data)
    # transactions_data = create_feature_vector3(transactions_data)

    # Initialize models
    rf1 = RandomForestClassifier(labelCol="fraud", featuresCol="features", numTrees=10)
    # rf2 = RandomForestClassifier(labelCol="fraud", featuresCol="features", numTrees=10)
    # rf3 = RandomForestClassifier(labelCol="fraud", featuresCol="features", numTrees=10)

    # Create pipelines
    pipeline1 = Pipeline(stages=[rf1])
    # pipeline2 = Pipeline(stages=[rf2])
    # pipeline3 = Pipeline(stages=[rf3])

    # Train models
    model1 = pipeline1.fit(fraud_data)
    # model2 = pipeline2.fit(credit_card_data)
    # model3 = pipeline3.fit(transactions_data)
    current_path = os.path.dirname(os.path.abspath(__file__))

    model1.write().overwrite().save(os.path.join(current_path, "models/rf_fraud_model"))
    # model2.write().overwrite().save(
    #     os.path.join(current_path, "models/rf_credit_card_model")
    # )
    # model3.write().overwrite().save(
    #     os.path.join(current_path, "models/rf_transactions_model")
    # )
    with mlflow.start_run() as run:
        print("START model retraining for Dataset 1")

        artifact_uri = run.info.artifact_uri
        print(f'artifact_uri={artifact_uri}')
        mlflow.spark.log_model(model1, "fraud_dataset1")
        num_trees = model1.stages[0].getNumTrees
        mlflow.log_metric("num_trees", num_trees)
        # Register the model
        model_uri = f"runs:/{run.info.run_id}/fraud_dataset1"
        print(f"Model URI: {model_uri}")
        mlflow.register_model(model_uri, "FraudDataset1")
        print("END model retraining for Dataset 1")
except Exception as e:
    print(f"Error training models: {e}")
    spark.stop()
finally:
    spark.stop()