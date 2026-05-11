
import os
import json
from pathlib import Path
from datetime import datetime

NOTEBOOK_DIR = Path("notebooks")

def md(text):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": text.strip().splitlines(True)
    }

def code(text):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": text.strip().splitlines(True)
    }

def read_file(path):
    path = Path(path)
    if path.exists():
        return path.read_text(encoding="utf-8", errors="ignore")
    return f"# Dosya bulunamadı: {path}\n# Bu hücreyi proje dizininde script çalıştırıldıktan sonra kontrol et."

def code_from_file(path):
    return code(read_file(path))

def notebook(path, cells):
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "pygments_lexer": "ipython3",
                "version": "3.11"
            },
            "created_by": "create_rich_notebooks.py",
            "created_at": datetime.now().isoformat(timespec="seconds")
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }
    Path(path).write_text(json.dumps(nb, ensure_ascii=False, indent=2), encoding="utf-8")

NOTEBOOK_DIR.mkdir(exist_ok=True)

# 01
notebook(NOTEBOOK_DIR / "01_docker_and_kafka_setup.ipynb", [
    md("""
# 01 - Docker ve Kafka Ortam Kurulumu

Bu notebook projenin altyapı katmanını açıklar. Amaç, tüm büyük veri bileşenlerini Docker Compose ile izole ve tekrar üretilebilir bir ortamda çalıştırmaktır.

## Bu adımda kurulan servisler

| Servis | Görevi |
|---|---|
| Kafka | Streaming veri akışı için mesaj kuyruğu |
| Producer | MovieLens rating verilerini Kafka'ya gönderen Python uygulaması |
| Spark | Kafka'dan veri okuyup Delta Lake'e yazan işleme motoru |
| MLflow | Model deney takibi |
| Dashboard | Gold katmanı sonuçlarını görselleştiren Streamlit arayüzü |

## Bu adımın proje mimarisindeki yeri

Docker ortamı, pipeline'ın temel altyapısıdır. Diğer bütün adımlar bu container'lar üzerinde çalışır.
"""),
    md("## docker-compose.yml\nAşağıdaki dosya tüm servisleri tek merkezden yönetir."),
    code_from_file("docker-compose.yml"),
    md("## Producer Dockerfile\nProducer servisi Python imajı üzerinde çalışır ve Kafka'ya veri gönderir."),
    code_from_file("producer/Dockerfile"),
    md("## Producer requirements.txt"),
    code_from_file("producer/requirements.txt"),
    md("## Spark Dockerfile\nSpark container içinde PySpark, Delta Lake ve MLflow bağımlılıkları bulunur."),
    code_from_file("spark/Dockerfile"),
    md("## Spark requirements.txt"),
    code_from_file("spark/requirements.txt"),
    md("## Dashboard Dockerfile\nDashboard Streamlit ile çalışır ve 8501 portundan yayın yapar."),
    code_from_file("dashboard/Dockerfile"),
    md("## Dashboard requirements.txt"),
    code_from_file("dashboard/requirements.txt"),
    md("""
## Çalıştırma komutları

Proje ana dizininde çalıştırılır.
"""),
    code("""
docker compose build
docker compose up -d kafka mlflow spark dashboard
docker ps
"""),
    md("""
## Beklenen çıktı

`docker ps` çıktısında aşağıdaki container'lar görülmelidir:

- movie-kafka
- movie-mlflow
- movie-spark
- movie-dashboard

Bu noktada altyapı hazırdır.
""")
])

# 02
notebook(NOTEBOOK_DIR / "02_streaming_to_bronze.ipynb", [
    md("""
# 02 - Kafka Streaming Verisini Bronze Delta Katmanına Yazma

Bu notebook, MovieLens `ratings.csv` verisinin streaming veri gibi Kafka'ya gönderilmesini ve Spark Structured Streaming ile Delta Bronze katmanına yazılmasını açıklar.

## Amaç

Gerçek dünyada bir film platformunda kullanıcılar sürekli rating üretir. Bu projede bu gerçek zamanlı veri akışı, CSV dosyasından okunan kayıtların Kafka'ya JSON mesajları olarak gönderilmesiyle simüle edilir.

## Veri akışı

`ratings.csv → Python Kafka Producer → Kafka topic: movie-ratings → Spark Structured Streaming → Delta Bronze`
"""),
    md("""
## Kafka Producer kodu

Bu kod `ratings.csv` dosyasını chunk'lar halinde okur. Her satırı JSON formatına çevirir ve Kafka'daki `movie-ratings` topic'ine gönderir.
"""),
    code_from_file("producer/kafka_producer.py"),
    md("""
## Producer kodunun işleyişi

1. Ortam değişkenlerinden Kafka adresi ve topic adı okunur.
2. `/app/data/raw/ml-25m/ratings.csv` dosyası Pandas ile chunk'lar halinde okunur.
3. Her satırdan `userId`, `movieId`, `rating`, `timestamp` alanları alınır.
4. Bu kayıt JSON mesajına çevrilir.
5. Kafka'ya gönderilir.
6. Her chunk sonunda kaç mesaj gönderildiği loglanır.
"""),
    md("""
## Spark Structured Streaming kodu

Bu kod Kafka topic'ini dinler, JSON mesajlarını parse eder ve Delta Bronze katmanına yazar.
"""),
    code_from_file("spark/streaming_to_delta.py"),
    md("""
## Spark streaming kodunun işleyişi

1. SparkSession Delta Lake desteğiyle başlatılır.
2. Kafka kaynağına bağlanılır.
3. `value` alanındaki JSON string parse edilir.
4. JSON için şema tanımlanır.
5. Unix timestamp okunabilir tarih alanına dönüştürülür.
6. Veri Delta formatında `/app/delta/bronze/ratings` path'ine append modunda yazılır.
7. Checkpoint kullanılarak streaming süreci güvenli hale getirilir.
"""),
    md("## Kafka topic oluşturma"),
    code("""
docker exec -it movie-kafka /opt/kafka/bin/kafka-topics.sh --create --topic movie-ratings --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec -it movie-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:29092
"""),
    md("## Spark streaming job'unu başlatma"),
    code("""
docker exec -it movie-spark bash
spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming_to_delta.py
"""),
    md("## Producer'ı çalıştırma"),
    code("""
docker compose up producer
"""),
    md("""
## Beklenen çıktı

Producer terminalinde:

`1000 rating Kafka'ya gönderildi.`

Delta klasöründe:

`delta/bronze/ratings/_delta_log` ve `.parquet` dosyaları oluşmalıdır.
""")
])

# 03
notebook(NOTEBOOK_DIR / "03_bronze_to_silver.ipynb", [
    md("""
# 03 - Bronze Katmanından Silver Katmanına Geçiş

Bu notebook Bronze katmanına yazılan ham rating verisinin kontrol edilmesini, ardından film bilgileriyle zenginleştirilerek Silver katmanına taşınmasını açıklar.

## Bronze ve Silver farkı

| Katman | Açıklama |
|---|---|
| Bronze | Kafka'dan gelen ham rating verisi |
| Silver | Temizlenmiş, duplicate kayıtları azaltılmış, film adı ve türleriyle zenginleştirilmiş veri |
"""),
    md("## Bronze kontrol script'i"),
    code_from_file("spark/check_bronze.py"),
    md("""
## Bronze kontrol kodunun işleyişi

1. Bronze Delta tablosu okunur.
2. Toplam kayıt sayısı hesaplanır.
3. Tablo şeması yazdırılır.
4. İlk 20 kayıt gösterilir.
5. Rating dağılımı hesaplanır.

Bu kontrol, streaming verinin gerçekten Delta Lake'e yazıldığını doğrulamak için kullanılır.
"""),
    md("## Bronze → Silver dönüşüm script'i"),
    code_from_file("spark/bronze_to_silver.py"),
    md("""
## Bronze to Silver kodunun işleyişi

1. Bronze rating tablosu okunur.
2. `movies.csv` dosyası okunur.
3. Null kayıtlar temizlenir.
4. Duplicate kayıtlar düşürülür.
5. Timestamp alanı gerçek zaman tipine çevrilir.
6. `genres` alanı `genres_array` olarak parçalanır.
7. Rating verisi ile film bilgisi `movieId` üzerinden join edilir.
8. Sonuç `/app/delta/silver/ratings_enriched` path'ine Delta formatında yazılır.
"""),
    md("## Çalıştırma komutları"),
    code("""
docker exec -it movie-spark bash

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 check_bronze.py

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 bronze_to_silver.py
"""),
    md("""
## Silver çıktısı

Silver tablosunda şu alanlar bulunur:

- userId
- movieId
- title
- genres
- genres_array
- rating
- timestamp
- rating_datetime

Bu veri artık EDA ve modelleme için daha anlamlıdır.
""")
])

# 04
notebook(NOTEBOOK_DIR / "04_silver_to_gold_eda.ipynb", [
    md("""
# 04 - Silver Katmanından Gold Katmanına EDA Tabloları

Bu notebook, Silver katmanındaki zenginleştirilmiş veriden analitik Gold tablolarının oluşturulmasını açıklar.

## Gold katmanının amacı

Gold katmanı dashboard ve makine öğrenmesi için hazır veri sağlar. Bu katmanda artık ham veri değil, analiz ve raporlama için hazırlanmış tablolar bulunur.
"""),
    md("## Silver to Gold script'i"),
    code_from_file("spark/silver_to_gold.py"),
    md("""
## Kodun işleyişi

Script aşağıdaki Gold tablolarını üretir:

### most_rated_movies
En çok puanlanan filmleri çıkarır. Film popülerliğini gösterir.

### top_rated_movies
Ortalama rating'e göre en başarılı filmleri çıkarır. Güvenilir sonuç için minimum rating sayısı filtresi kullanılır.

### genre_rating_stats
Film türlerine göre rating sayısı ve ortalama rating değerlerini üretir.

### user_activity_stats
Kullanıcı bazlı rating sayısı ve kullanıcı ortalama rating değerlerini üretir.

### ml_ratings_dataset
Modelleme için sadeleştirilmiş `userId`, `movieId`, `rating` tablosudur.
"""),
    md("## Çalıştırma komutu"),
    code("""
docker exec -it movie-spark bash
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 silver_to_gold.py
"""),
    md("""
## Oluşan Gold path'leri

- `/app/delta/gold/most_rated_movies`
- `/app/delta/gold/top_rated_movies`
- `/app/delta/gold/genre_rating_stats`
- `/app/delta/gold/user_activity_stats`
- `/app/delta/gold/ml_ratings_dataset`

Dashboard'un ilk EDA grafikleri bu tablolardan beslenir.
""")
])

# 05
notebook(NOTEBOOK_DIR / "05_feature_engineering.ipynb", [
    md("""
# 05 - Feature Engineering

Bu notebook, makine öğrenmesi modelleri için anlamlı özelliklerin nasıl üretildiğini açıklar.

## Amaç

Ham rating verisini doğrudan modele vermek yerine kullanıcı davranışı, film popülerliği, film kalitesi, zaman bilgisi ve tür bilgisi gibi özellikler üretmek.
"""),
    md("## Feature Engineering script'i"),
    code_from_file("spark/feature_engineering.py"),
    md("""
## Üretilen feature'lar ve iş mantığı

| Feature | Açıklama |
|---|---|
| rating_year | Rating'in verildiği yıl |
| rating_month | Rating'in verildiği ay |
| rating_dayofweek | Haftanın günü |
| rating_hour | Günün saati |
| genre_count | Filmin kaç türe ait olduğu |
| user_rating_count | Kullanıcının toplam rating sayısı |
| user_avg_rating | Kullanıcının ortalama puan verme eğilimi |
| movie_rating_count | Filmin aldığı toplam rating sayısı |
| movie_avg_rating | Filmin genel beğeni seviyesi |
| main_genre_index | Ana türün sayısal temsili |
| main_genre_vec | Ana türün one-hot encoded vektörü |

Bu özellikler modelin rating tahminini yalnızca kullanıcı-film ID ilişkisine değil, davranışsal ve içeriksel bilgilere de dayandırmasını sağlar.
"""),
    md("""
## Kodun işleyişi

1. Silver Delta tablosu okunur.
2. Zaman özellikleri çıkarılır.
3. Kullanıcı bazlı istatistikler hesaplanır.
4. Film bazlı istatistikler hesaplanır.
5. Film türleri işlenir.
6. Kategorik tür bilgisi StringIndexer ve OneHotEncoder ile sayısal hale getirilir.
7. VectorAssembler ile tüm özellikler `features` vektöründe birleştirilir.
8. Son tablo Delta Gold katmanına yazılır.
"""),
    md("## Çalıştırma komutu"),
    code("""
docker exec -it movie-spark bash
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 feature_engineering.py
"""),
    md("""
## Çıktı

`/app/delta/gold/ml_features_dataset`

Bu tablo 5 model karşılaştırması için ana veri kaynağıdır.
""")
])

# 06
notebook(NOTEBOOK_DIR / "06_model_training_mlflow.ipynb", [
    md("""
# 06 - Model Eğitimi ve MLflow Deney Takibi

Bu notebook iki farklı modelleme yaklaşımını açıklar:

1. ALS tabanlı öneri/rating tahmin modeli
2. Feature engineering sonrası 5 regresyon modelinin karşılaştırılması

## Değerlendirme metrikleri

- RMSE
- MAE
- R²

Tüm deneyler MLflow'a loglanır.
"""),
    md("## ALS modeli script'i"),
    code_from_file("spark/train_model.py"),
    md("""
## ALS kodunun işleyişi

1. Gold katmanındaki `ml_ratings_dataset` okunur.
2. Veri train/test olarak ayrılır.
3. Spark MLlib ALS modeli eğitilir.
4. Tahminler alınır.
5. RMSE ve MAE hesaplanır.
6. Model ve metrikler MLflow'a loglanır.

ALS öneri sistemleri için uygundur çünkü kullanıcı-film-rating matrisinden gizli faktörler öğrenir.
"""),
    md("## 5 model karşılaştırma script'i"),
    code_from_file("spark/train_regression_models.py"),
    md("""
## 5 model kodunun işleyişi

1. `ml_features_dataset` okunur.
2. Train/test split yapılır.
3. 5 regresyon modeli sırayla eğitilir:
   - Linear Regression
   - Decision Tree Regressor
   - Random Forest Regressor
   - GBT Regressor
   - Generalized Linear Regression
4. Her model için RMSE, MAE ve R² hesaplanır.
5. Her model MLflow'a ayrı run olarak loglanır.
6. Tree-based modeller için feature importance çıkarılır.
7. En iyi model RMSE değerine göre seçilir.
8. En iyi modelin gerçek/tahmin/residual sonuçları Gold katmanına yazılır.
"""),
    md("## Çalıştırma komutları"),
    code("""
docker exec -it movie-spark bash

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 train_model.py

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 train_regression_models.py
"""),
    md("""
## Oluşan çıktılar

- `/app/delta/gold/model_comparison_results`
- `/app/delta/gold/feature_importance_results`
- `/app/delta/gold/best_model_predictions`

## MLflow arayüzü

`http://localhost:5000`

Burada model parametreleri, metrikler ve model artifact'leri incelenebilir.
""")
])

# 07
notebook(NOTEBOOK_DIR / "07_dashboard_summary.ipynb", [
    md("""
# 07 - Dashboard ve Görselleştirme

Bu notebook Streamlit dashboard'unun nasıl çalıştığını ve hangi Gold tablolarından beslendiğini açıklar.

## Dashboard amacı

Pipeline sonucunda üretilen Gold tablolarını görselleştirerek sonuçları anlaşılır bir arayüzde sunmak.
"""),
    md("## Dashboard kodu"),
    code_from_file("dashboard/app.py"),
    md("""
## Dashboard kodunun işleyişi

1. `/app/delta/gold` altındaki parquet dosyaları okunur.
2. KPI değerleri hesaplanır:
   - Toplam rating
   - Tekil film
   - Tekil kullanıcı
   - Ortalama rating
3. EDA grafikleri oluşturulur:
   - En çok puanlanan filmler
   - En başarılı filmler
   - Tür bazlı rating analizi
   - Kullanıcı aktivitesi
   - Rating dağılımı
4. Model sonuçları görselleştirilir:
   - 5 model performans karşılaştırması
   - Feature importance
   - Gerçek vs tahmin scatter plot
   - Residual histogram ve residual scatter plot
5. Sidebar filtreleri ile grafiklerde gösterilecek kayıt sayısı kontrol edilir.
"""),
    md("""
## Dashboard'da kullanılan Gold tabloları

- `most_rated_movies`
- `top_rated_movies`
- `genre_rating_stats`
- `user_activity_stats`
- `ml_ratings_dataset`
- `model_comparison_results`
- `feature_importance_results`
- `best_model_predictions`
"""),
    md("## Çalıştırma komutları"),
    code("""
docker compose build dashboard
docker compose up -d dashboard
"""),
    md("""
## Dashboard adresi

`http://localhost:8501`

Bu ekran sunumda projenin son kullanıcıya/analiste dönük görsel arayüzü olarak gösterilir.
""")
])

print("Geliştirilmiş notebooklar oluşturuldu.")
print(f"Klasör: {NOTEBOOK_DIR.resolve()}")
