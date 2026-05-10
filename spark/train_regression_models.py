import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand
from pyspark.ml.regression import (
    LinearRegression,
    DecisionTreeRegressor,
    RandomForestRegressor,
    GBTRegressor,
    GeneralizedLinearRegression
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Row


FEATURE_DATASET_PATH = "/app/delta/gold/ml_features_dataset"
MODEL_RESULTS_PATH = "/app/delta/gold/model_comparison_results"
FEATURE_IMPORTANCE_PATH = "/app/delta/gold/feature_importance_results"
BEST_MODEL_PREDICTIONS_PATH = "/app/delta/gold/best_model_predictions"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


spark = (
    SparkSession.builder
    .appName("MovieLensRegressionModelComparison")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("MovieLens Regression Model Comparison")


print("Feature dataset okunuyor...")
df = spark.read.format("delta").load(FEATURE_DATASET_PATH)

df = (
    df
    .select(
        col("userId"),
        col("movieId"),
        col("title"),
        col("rating").alias("label"),
        col("features")
    )
    .dropna()
)

print("Toplam kayıt sayısı:")
print(df.count())

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

rmse_evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)

mae_evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="mae"
)

r2_evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="r2"
)


feature_names = [
    "rating_year",
    "rating_month",
    "rating_dayofweek",
    "rating_hour",
    "genre_count",
    "user_rating_count",
    "user_avg_rating",
    "movie_rating_count",
    "movie_avg_rating",
    "main_genre_vec"
]


models = [
    (
        "Linear Regression",
        LinearRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=50,
            regParam=0.1
        ),
        {
            "maxIter": 50,
            "regParam": 0.1
        }
    ),
    (
        "Decision Tree Regressor",
        DecisionTreeRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxDepth=8,
            seed=42
        ),
        {
            "maxDepth": 8
        }
    ),
    (
        "Random Forest Regressor",
        RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            numTrees=50,
            maxDepth=8,
            seed=42
        ),
        {
            "numTrees": 50,
            "maxDepth": 8
        }
    ),
    (
        "GBT Regressor",
        GBTRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            maxIter=50,
            maxDepth=5,
            seed=42
        ),
        {
            "maxIter": 50,
            "maxDepth": 5
        }
    ),
    (
        "Generalized Linear Regression",
        GeneralizedLinearRegression(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            family="gaussian",
            link="identity",
            maxIter=50,
            regParam=0.1
        ),
        {
            "family": "gaussian",
            "link": "identity",
            "maxIter": 50,
            "regParam": 0.1
        }
    )
]


model_result_rows = []
feature_importance_rows = []

best_rmse = None
best_model_name = None
best_predictions_df = None


for model_name, estimator, params in models:
    print("=" * 80)
    print(f"Model eğitiliyor: {model_name}")

    with mlflow.start_run(run_name=model_name):
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("dataset", "ml_features_dataset")

        for param_name, param_value in params.items():
            mlflow.log_param(param_name, param_value)

        model = estimator.fit(train_df)
        predictions = model.transform(test_df)

        rmse = rmse_evaluator.evaluate(predictions)
        mae = mae_evaluator.evaluate(predictions)
        r2 = r2_evaluator.evaluate(predictions)

        print(f"{model_name} RMSE: {rmse}")
        print(f"{model_name} MAE: {mae}")
        print(f"{model_name} R2: {r2}")

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)

        mlflow.spark.log_model(model, "model")

        model_result_rows.append(
            Row(
                model_name=model_name,
                rmse=float(rmse),
                mae=float(mae),
                r2=float(r2)
            )
        )

        if best_rmse is None or rmse < best_rmse:
            best_rmse = rmse
            best_model_name = model_name

            best_predictions_df = (
                predictions
                .select(
                    col("userId"),
                    col("movieId"),
                    col("title"),
                    col("label").alias("actual_rating"),
                    col("prediction").alias("predicted_rating")
                )
                .withColumn("residual", col("actual_rating") - col("predicted_rating"))
                .withColumn("model_name", lit(model_name))
            )

        if hasattr(model, "featureImportances"):
            importances = model.featureImportances.toArray().tolist()

            for index, importance in enumerate(importances):
                feature_name = feature_names[index] if index < len(feature_names) else f"feature_{index}"

                feature_importance_rows.append(
                    Row(
                        model_name=model_name,
                        feature_name=feature_name,
                        importance=float(importance)
                    )
                )


print("=" * 80)
print("Model karşılaştırma sonuçları oluşturuluyor...")

model_results_df = spark.createDataFrame(model_result_rows)

model_results_df = model_results_df.orderBy(col("rmse").asc())

model_results_df.show(truncate=False)

print("Model sonuçları Delta formatında yazılıyor...")

(
    model_results_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(MODEL_RESULTS_PATH)
)


if feature_importance_rows:
    feature_importance_df = spark.createDataFrame(feature_importance_rows)

    feature_importance_df = feature_importance_df.orderBy(
        col("model_name"),
        col("importance").desc()
    )

    print("Feature importance sonuçları:")
    feature_importance_df.show(100, truncate=False)

    (
        feature_importance_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(FEATURE_IMPORTANCE_PATH)
    )

    print(f"Feature importance path: {FEATURE_IMPORTANCE_PATH}")
else:
    print("Bu modellerden feature importance çıkarılamadı.")


if best_predictions_df is not None:
    print("=" * 80)
    print(f"En iyi model: {best_model_name}")
    print(f"En iyi RMSE: {best_rmse}")
    print("En iyi model tahmin sonuçları yazılıyor...")

    best_predictions_sample_df = (
        best_predictions_df
        .orderBy(rand(seed=42))
        .limit(5000)
    )

    best_predictions_sample_df.show(20, truncate=False)

    (
        best_predictions_sample_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BEST_MODEL_PREDICTIONS_PATH)
    )

    print(f"Best model predictions path: {BEST_MODEL_PREDICTIONS_PATH}")


print("5 model karşılaştırması tamamlandı.")
print(f"Model comparison path: {MODEL_RESULTS_PATH}")
print(f"Feature importance path: {FEATURE_IMPORTANCE_PATH}")
print(f"Best model predictions path: {BEST_MODEL_PREDICTIONS_PATH}")