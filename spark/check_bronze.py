from pyspark.sql import SparkSession


BRONZE_PATH = "/app/delta/bronze/ratings"

spark = (
    SparkSession.builder
    .appName("CheckBronzeDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load(BRONZE_PATH)

print("Bronze tablosundaki toplam kayıt sayısı:")
print(df.count())

print("Şema:")
df.printSchema()

print("İlk 20 kayıt:")
df.show(20, truncate=False)

print("Rating dağılımı:")
df.groupBy("rating").count().orderBy("rating").show()