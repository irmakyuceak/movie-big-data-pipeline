import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


BRONZE_PATH = "/app/delta/bronze/ratings"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


spark = (
    SparkSession.builder
    .appName("MovieRatingPredictionALS")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("MovieLens Rating Prediction")

ratings_df = spark.read.format("delta").load(BRONZE_PATH)

ratings_df = ratings_df.select(
    "userId",
    "movieId",
    "rating"
).dropna()

train_df, test_df = ratings_df.randomSplit([0.8, 0.2], seed=42)

with mlflow.start_run(run_name="ALS_baseline_model"):
    rank = 10
    max_iter = 5
    reg_param = 0.1

    als = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        coldStartStrategy="drop",
        nonnegative=True
    )

    model = als.fit(train_df)

    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )

    rmse = evaluator.evaluate(predictions)

    mlflow.log_param("model", "ALS")
    mlflow.log_param("rank", rank)
    mlflow.log_param("maxIter", max_iter)
    mlflow.log_param("regParam", reg_param)
    mlflow.log_metric("rmse", rmse)

    mlflow.spark.log_model(model, "als_model")

    print(f"Model eğitildi. RMSE: {rmse}")