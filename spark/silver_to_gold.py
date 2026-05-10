from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, explode, round


SILVER_PATH = "/app/delta/silver/ratings_enriched"

GOLD_TOP_RATED_PATH = "/app/delta/gold/top_rated_movies"
GOLD_MOST_RATED_PATH = "/app/delta/gold/most_rated_movies"
GOLD_GENRE_STATS_PATH = "/app/delta/gold/genre_rating_stats"
GOLD_USER_ACTIVITY_PATH = "/app/delta/gold/user_activity_stats"
GOLD_ML_DATASET_PATH = "/app/delta/gold/ml_ratings_dataset"


spark = (
    SparkSession.builder
    .appName("SilverToGoldMovieAnalytics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Silver Delta verisi okunuyor...")
silver_df = spark.read.format("delta").load(SILVER_PATH)

print("Gold tabloları oluşturuluyor...")


# 1. En çok puanlanan filmler
most_rated_movies_df = (
    silver_df
    .groupBy("movieId", "title", "genres")
    .agg(
        count("*").alias("rating_count"),
        round(avg("rating"), 2).alias("avg_rating")
    )
    .orderBy(col("rating_count").desc())
)

print("En çok puanlanan filmler:")
most_rated_movies_df.show(20, truncate=False)


# 2. Ortalama rating'e göre en başarılı filmler
# Daha güvenilir sonuç için en az 30 rating almış filmleri dikkate alıyoruz.
top_rated_movies_df = (
    silver_df
    .groupBy("movieId", "title", "genres")
    .agg(
        count("*").alias("rating_count"),
        round(avg("rating"), 2).alias("avg_rating")
    )
    .filter(col("rating_count") >= 30)
    .orderBy(col("avg_rating").desc(), col("rating_count").desc())
)

print("Ortalama rating'e göre en başarılı filmler:")
top_rated_movies_df.show(20, truncate=False)


# 3. Tür bazlı ortalama rating analizi
genre_stats_df = (
    silver_df
    .withColumn("genre", explode(col("genres_array")))
    .groupBy("genre")
    .agg(
        count("*").alias("rating_count"),
        round(avg("rating"), 2).alias("avg_rating")
    )
    .orderBy(col("avg_rating").desc())
)

print("Tür bazlı rating istatistikleri:")
genre_stats_df.show(50, truncate=False)


# 4. Kullanıcı bazlı aktivite analizi
user_activity_df = (
    silver_df
    .groupBy("userId")
    .agg(
        count("*").alias("rating_count"),
        round(avg("rating"), 2).alias("avg_user_rating")
    )
    .orderBy(col("rating_count").desc())
)

print("En aktif kullanıcılar:")
user_activity_df.show(20, truncate=False)


# 5. ML modeli için hazır dataset
ml_dataset_df = (
    silver_df
    .select(
        col("userId"),
        col("movieId"),
        col("rating")
    )
    .dropna()
    .dropDuplicates(["userId", "movieId"])
)

print("ML dataset örneği:")
ml_dataset_df.show(20, truncate=False)

print("ML dataset toplam kayıt sayısı:")
print(ml_dataset_df.count())


print("Gold tabloları Delta formatında yazılıyor...")

most_rated_movies_df.write.format("delta").mode("overwrite").save(GOLD_MOST_RATED_PATH)
top_rated_movies_df.write.format("delta").mode("overwrite").save(GOLD_TOP_RATED_PATH)
genre_stats_df.write.format("delta").mode("overwrite").save(GOLD_GENRE_STATS_PATH)
user_activity_df.write.format("delta").mode("overwrite").save(GOLD_USER_ACTIVITY_PATH)
ml_dataset_df.write.format("delta").mode("overwrite").save(GOLD_ML_DATASET_PATH)

print("Gold katmanı başarıyla oluşturuldu.")
print(f"Most rated movies path: {GOLD_MOST_RATED_PATH}")
print(f"Top rated movies path: {GOLD_TOP_RATED_PATH}")
print(f"Genre stats path: {GOLD_GENRE_STATS_PATH}")
print(f"User activity path: {GOLD_USER_ACTIVITY_PATH}")
print(f"ML dataset path: {GOLD_ML_DATASET_PATH}")