import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


ML_DATASET_PATH = "/app/delta/gold/ml_ratings_dataset"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


spark = (
    SparkSession.builder
    .appName("MovieLensALSRecommendationModel")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("MovieLens ALS Recommendation")

print("Gold ML dataset okunuyor...")
ratings_df = spark.read.format("delta").load(ML_DATASET_PATH)

ratings_df = (
    ratings_df
    .select("userId", "movieId", "rating")
    .dropna()
)

print("Toplam kayıt sayısı:")
print(ratings_df.count())

train_df, test_df = ratings_df.randomSplit([0.8, 0.2], seed=42)

with mlflow.start_run(run_name="ALS_gold_baseline_model"):
    rank = 10
    max_iter = 10
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

    print("ALS modeli eğitiliyor...")
    model = als.fit(train_df)

    print("Test verisi üzerinde tahmin yapılıyor...")
    predictions = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )

    evaluator_mae = RegressionEvaluator(
        metricName="mae",
        labelCol="rating",
        predictionCol="prediction"
    )

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)

    mlflow.log_param("model", "ALS")
    mlflow.log_param("dataset", "gold_ml_ratings_dataset")
    mlflow.log_param("rank", rank)
    mlflow.log_param("maxIter", max_iter)
    mlflow.log_param("regParam", reg_param)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)

    mlflow.spark.log_model(model, "als_model")

    print("Model eğitimi tamamlandı.")
    print(f"RMSE: {rmse}")
    print(f"MAE: {mae}")

    print("Örnek tahminler:")
    predictions.select(
        "userId",
        "movieId",
        "rating",
        "prediction"
    ).show(20, truncate=False)