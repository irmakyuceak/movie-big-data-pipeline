import os
import json


NOTEBOOK_DIR = "notebooks"


def markdown_cell(source):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.strip().splitlines(True)
    }


def code_cell(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.strip().splitlines(True)
    }


def create_notebook(path, cells):
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.11"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }

    with open(path, "w", encoding="utf-8") as file:
        json.dump(notebook, file, ensure_ascii=False, indent=2)


os.makedirs(NOTEBOOK_DIR, exist_ok=True)


notebooks = {
    "01_docker_and_kafka_setup.ipynb": [
        markdown_cell("""
# 01 - Docker ve Kafka Ortam Kurulumu

Bu notebook, projenin Docker Compose tabanlı altyapı kurulumunu açıklar.

Bu adımda amaç:
- Kafka servisinin ayağa kaldırılması
- MLflow servisinin başlatılması
- Spark container ortamının hazırlanması
- Producer, Spark, MLflow ve Dashboard servislerinin Docker ile izole şekilde çalıştırılmasıdır.
"""),
        markdown_cell("""
## Kullanılan Servisler

- `movie-kafka`: Apache Kafka broker
- `movie-producer`: Python Kafka Producer
- `movie-spark`: Apache Spark çalışma ortamı
- `movie-mlflow`: MLflow tracking server
- `movie-dashboard`: Streamlit dashboard
"""),
        code_cell("""
# Proje ana dizininde çalıştırılır:
docker compose build
"""),
        code_cell("""
# Ana servisleri başlatma:
docker compose up -d kafka mlflow spark
"""),
        code_cell("""
# Çalışan container'ları kontrol etme:
docker ps
"""),
        markdown_cell("""
## Beklenen Çıktı

`docker ps` sonucunda aşağıdaki container'ların çalıştığı görülmelidir:

- movie-kafka
- movie-mlflow
- movie-spark
""")
    ],

    "02_streaming_to_bronze.ipynb": [
        markdown_cell("""
# 02 - Kafka Streaming Verisini Bronze Delta Katmanına Yazma

Bu notebook, MovieLens rating verilerinin Kafka üzerinden streaming veri olarak üretilmesini ve Spark Structured Streaming ile Delta Lake Bronze katmanına yazılmasını açıklar.
"""),
        markdown_cell("""
## Amaç

- `ratings.csv` dosyasındaki verileri Python Kafka Producer ile Kafka'ya göndermek
- Kafka topic'inden Spark Structured Streaming ile JSON mesajlarını okumak
- Veriyi parse edip Delta Lake Bronze katmanına yazmak
"""),
        code_cell("""
# Kafka topic oluşturma:
docker exec -it movie-kafka /opt/kafka/bin/kafka-topics.sh --create --topic movie-ratings --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
"""),
        code_cell("""
# Spark container içine girme:
docker exec -it movie-spark bash
"""),
        code_cell("""
# Container içinde Spark streaming job'u başlatma:
spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming_to_delta.py
"""),
        code_cell("""
# Yeni terminalde producer'ı çalıştırma:
docker compose up producer
"""),
        markdown_cell("""
## Bronze Çıktısı

Bronze verisi aşağıdaki path'e yazılır:

`delta/bronze/ratings`

Bu klasörde `_delta_log` ve `.parquet` dosyalarının oluşması beklenir.
""")
    ],

    "03_bronze_to_silver.ipynb": [
        markdown_cell("""
# 03 - Bronze Katmanından Silver Katmanına Geçiş

Bu notebook, Bronze katmanındaki ham rating verisinin temizlenerek ve film bilgileriyle zenginleştirilerek Silver katmanına yazılmasını açıklar.
"""),
        markdown_cell("""
## Yapılan İşlemler

- Bronze Delta tablosu okunur.
- `movies.csv` dosyası okunur.
- Rating verisi ile film bilgileri `movieId` üzerinden join edilir.
- Null ve duplicate kayıtlar temizlenir.
- `genres` alanı dizi formatına çevrilir.
- Sonuç Delta Silver katmanına yazılır.
"""),
        code_cell("""
# Spark container içinde çalıştırılır:
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 bronze_to_silver.py
"""),
        markdown_cell("""
## Silver Çıktısı

Silver verisi aşağıdaki path'e yazılır:

`delta/silver/ratings_enriched`

Silver tablosu şu alanları içerir:

- userId
- movieId
- title
- genres
- genres_array
- rating
- timestamp
- rating_datetime
""")
    ],

    "04_silver_to_gold_eda.ipynb": [
        markdown_cell("""
# 04 - Silver Katmanından Gold Katmanına EDA Tabloları

Bu notebook, Silver katmanındaki zenginleştirilmiş veriden analitik Gold tablolarının oluşturulmasını açıklar.
"""),
        markdown_cell("""
## Oluşturulan Gold Tabloları

- `most_rated_movies`
- `top_rated_movies`
- `genre_rating_stats`
- `user_activity_stats`
- `ml_ratings_dataset`
"""),
        code_cell("""
# Spark container içinde çalıştırılır:
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 silver_to_gold.py
"""),
        markdown_cell("""
## EDA Çıktıları

Bu adımda:
- En çok puanlanan filmler
- Ortalama rating'e göre en başarılı filmler
- Tür bazlı rating istatistikleri
- En aktif kullanıcılar
- ML modeli için sadeleştirilmiş kullanıcı-film-rating veri seti

oluşturulmuştur.
""")
    ],

    "05_feature_engineering.ipynb": [
        markdown_cell("""
# 05 - Feature Engineering

Bu notebook, makine öğrenmesi modelleri için anlamlı özelliklerin oluşturulmasını açıklar.
"""),
        markdown_cell("""
## Üretilen Özellikler

Bu projede en az 5 feature şartını karşılamak için aşağıdaki özellikler üretilmiştir:

- rating_year
- rating_month
- rating_dayofweek
- rating_hour
- genre_count
- user_rating_count
- user_avg_rating
- movie_rating_count
- movie_avg_rating
- main_genre_index
- main_genre_vec
"""),
        markdown_cell("""
## Özelliklerin İş Mantığı

- Kullanıcı ortalama puanı, kullanıcının genel puanlama eğilimini temsil eder.
- Film ortalama puanı, filmin genel beğeni seviyesini temsil eder.
- Kullanıcı rating sayısı, kullanıcının platformdaki aktivitesini gösterir.
- Film rating sayısı, filmin popülerliğini gösterir.
- Zaman özellikleri, puanlama davranışının dönemsel değişimini yakalamak için kullanılır.
- Genre bilgisi, film türünün rating üzerindeki etkisini modele dahil eder.
"""),
        code_cell("""
# Spark container içinde çalıştırılır:
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 feature_engineering.py
"""),
        markdown_cell("""
## Çıktı

Feature Engineering sonucunda oluşturulan tablo:

`delta/gold/ml_features_dataset`
""")
    ],

    "06_model_training_mlflow.ipynb": [
        markdown_cell("""
# 06 - Model Eğitimi ve MLflow Deney Takibi

Bu notebook, feature engineering sonrası 5 farklı regresyon modelinin eğitilmesini ve MLflow ile takip edilmesini açıklar.
"""),
        markdown_cell("""
## Kullanılan Modeller

Regresyon problemi için aşağıdaki 5 model eğitilmiştir:

1. Linear Regression
2. Decision Tree Regressor
3. Random Forest Regressor
4. Gradient Boosted Trees Regressor
5. Generalized Linear Regression
"""),
        markdown_cell("""
## Kullanılan Metrikler

- RMSE
- MAE
- R²

Ayrıca feature importance analizi yapılmış ve en iyi modelin tahmin sonuçları kaydedilmiştir.
"""),
        code_cell("""
# Spark container içinde çalıştırılır:
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 train_regression_models.py
"""),
        markdown_cell("""
## Oluşturulan Çıktılar

- `delta/gold/model_comparison_results`
- `delta/gold/feature_importance_results`
- `delta/gold/best_model_predictions`

MLflow arayüzü:

`http://localhost:5000`
""")
    ],

    "07_dashboard_summary.ipynb": [
        markdown_cell("""
# 07 - Dashboard ve Sonuçların Görselleştirilmesi

Bu notebook, proje sonuçlarının Streamlit dashboard ile nasıl görselleştirildiğini açıklar.
"""),
        markdown_cell("""
## Dashboard İçeriği

Dashboard üzerinde aşağıdaki görseller bulunmaktadır:

- KPI kartları
- En çok puanlanan filmler
- Ortalama rating'e göre en başarılı filmler
- Tür bazlı rating analizi
- Kullanıcı aktivite analizi
- Rating dağılımı
- 5 model performans karşılaştırması
- Feature importance grafiği
- Gerçek vs tahmin scatter plot
- Residual dağılım grafiği
"""),
        code_cell("""
# Dashboard'u build etme:
docker compose build dashboard
"""),
        code_cell("""
# Dashboard'u çalıştırma:
docker compose up -d dashboard
"""),
        markdown_cell("""
## Dashboard Adresi

`http://localhost:8501`

Bu dashboard, Delta Lake Gold katmanındaki tabloları okuyarak görselleştirme yapmaktadır.
""")
    ]
}


for filename, cells in notebooks.items():
    path = os.path.join(NOTEBOOK_DIR, filename)
    create_notebook(path, cells)
    print(f"Created: {path}")

print("Notebook dosyaları başarıyla oluşturuldu.")