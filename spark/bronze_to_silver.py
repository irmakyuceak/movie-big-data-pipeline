from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, split, trim


BRONZE_PATH = "/app/delta/bronze/ratings"
SILVER_PATH = "/app/delta/silver/ratings_enriched"
MOVIES_PATH = "/app/data/raw/ml-25m/movies.csv"


spark = (
    SparkSession.builder
    .appName("BronzeToSilverMovieRatings")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("Bronze rating verisi okunuyor...")
ratings_df = spark.read.format("delta").load(BRONZE_PATH)

print("Movies CSV okunuyor...")
movies_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(MOVIES_PATH)
)

print("Veri temizleme ve zenginleştirme işlemleri başlıyor...")

ratings_clean_df = (
    ratings_df
    .dropna(subset=["userId", "movieId", "rating", "timestamp"])
    .dropDuplicates(["userId", "movieId", "timestamp"])
    .withColumn("rating_datetime", from_unixtime(col("timestamp")).cast("timestamp"))
)

movies_clean_df = (
    movies_df
    .dropna(subset=["movieId", "title", "genres"])
    .dropDuplicates(["movieId"])
    .withColumn("genres_array", split(col("genres"), "\\|"))
)

silver_df = (
    ratings_clean_df
    .join(movies_clean_df, on="movieId", how="inner")
    .select(
        col("userId"),
        col("movieId"),
        col("title"),
        col("genres"),
        col("genres_array"),
        col("rating"),
        col("timestamp"),
        col("rating_datetime")
    )
)

print("Silver veri örneği:")
silver_df.show(20, truncate=False)

print("Silver toplam kayıt sayısı:")
print(silver_df.count())

print("Silver Delta katmanına yazılıyor...")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .save(SILVER_PATH)
)

print("Silver katmanı başarıyla oluşturuldu.")
print(f"Silver path: {SILVER_PATH}")