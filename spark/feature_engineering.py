from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    year,
    month,
    dayofweek,
    hour,
    size,
    explode,
    round
)
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler


SILVER_PATH = "/app/delta/silver/ratings_enriched"
FEATURE_TABLE_PATH = "/app/delta/gold/ml_features_dataset"


spark = (
    SparkSession.builder
    .appName("MovieLensFeatureEngineering")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Silver Delta verisi okunuyor...")
silver_df = spark.read.format("delta").load(SILVER_PATH)

silver_df = silver_df.dropna(subset=["userId", "movieId", "rating", "rating_datetime", "genres_array"])

print("Temel zaman özellikleri oluşturuluyor...")

base_df = (
    silver_df
    .withColumn("rating_year", year(col("rating_datetime")))
    .withColumn("rating_month", month(col("rating_datetime")))
    .withColumn("rating_dayofweek", dayofweek(col("rating_datetime")))
    .withColumn("rating_hour", hour(col("rating_datetime")))
    .withColumn("genre_count", size(col("genres_array")))
)

print("Kullanıcı bazlı özellikler oluşturuluyor...")

user_features_df = (
    base_df
    .groupBy("userId")
    .agg(
        count("*").alias("user_rating_count"),
        round(avg("rating"), 3).alias("user_avg_rating")
    )
)

print("Film bazlı özellikler oluşturuluyor...")

movie_features_df = (
    base_df
    .groupBy("movieId")
    .agg(
        count("*").alias("movie_rating_count"),
        round(avg("rating"), 3).alias("movie_avg_rating")
    )
)

print("Ana genre özelliği oluşturuluyor...")

genre_exploded_df = (
    base_df
    .withColumn("main_genre", explode(col("genres_array")))
    .groupBy("movieId", "main_genre")
    .agg(count("*").alias("genre_occurrence"))
)

# Her film için en çok görünen genre değerlerinden birini ana genre gibi kullanıyoruz.
# Bu veri setinde genres_array film başına sabit olduğu için pratikte ilk türlerden birini temsil eder.
main_genre_df = (
    genre_exploded_df
    .dropDuplicates(["movieId"])
    .select("movieId", "main_genre")
)

print("Tüm feature tabloları birleştiriliyor...")

feature_df = (
    base_df
    .join(user_features_df, on="userId", how="left")
    .join(movie_features_df, on="movieId", how="left")
    .join(main_genre_df, on="movieId", how="left")
    .select(
        "userId",
        "movieId",
        "title",
        "genres",
        "main_genre",
        "rating",
        "rating_year",
        "rating_month",
        "rating_dayofweek",
        "rating_hour",
        "genre_count",
        "user_rating_count",
        "user_avg_rating",
        "movie_rating_count",
        "movie_avg_rating"
    )
    .dropna()
    .dropDuplicates(["userId", "movieId"])
)

print("Kategorik main_genre değişkeni sayısal hale getiriliyor...")

genre_indexer = StringIndexer(
    inputCol="main_genre",
    outputCol="main_genre_index",
    handleInvalid="keep"
)

indexed_df = genre_indexer.fit(feature_df).transform(feature_df)

encoder = OneHotEncoder(
    inputCols=["main_genre_index"],
    outputCols=["main_genre_vec"]
)

encoded_df = encoder.fit(indexed_df).transform(indexed_df)

print("Model için features vektörü oluşturuluyor...")

feature_columns = [
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

assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features",
    handleInvalid="keep"
)

final_df = assembler.transform(encoded_df)

final_df = final_df.select(
    "userId",
    "movieId",
    "title",
    "genres",
    "main_genre",
    "rating",
    "rating_year",
    "rating_month",
    "rating_dayofweek",
    "rating_hour",
    "genre_count",
    "user_rating_count",
    "user_avg_rating",
    "movie_rating_count",
    "movie_avg_rating",
    "main_genre_index",
    "main_genre_vec",
    "features"
)

print("Feature Engineering sonucu örnek veri:")
final_df.show(20, truncate=False)

print("Toplam kayıt sayısı:")
print(final_df.count())

print("Feature dataset Delta formatında yazılıyor...")

(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(FEATURE_TABLE_PATH)
)

print("Feature Engineering tamamlandı.")
print(f"Feature table path: {FEATURE_TABLE_PATH}")