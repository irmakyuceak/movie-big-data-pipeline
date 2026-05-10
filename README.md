# MovieLens Big Data Pipeline

Bu proje, MovieLens 25M veri seti kullanılarak geliştirilmiş uçtan uca bir büyük veri pipeline projesidir. Projede rating verileri gerçek zamanlı bir film platformundan geliyormuş gibi Apache Kafka üzerinden stream edilmiştir. Apache Spark ile veri işleme yapılmış, Delta Lake üzerinde Bronze/Silver/Gold katmanlı veri mimarisi kurulmuş, Spark MLlib ALS algoritması ile rating tahmin modeli eğitilmiş ve model deneyleri MLflow üzerinden takip edilmiştir. Ayrıca Gold katmanında oluşturulan analitik tablolar Streamlit dashboard ile görselleştirilmiştir.

---

## Projenin Amacı

Bu projenin amacı, büyük veri ekosisteminde sık kullanılan teknolojileri bir araya getirerek gerçek dünya senaryosuna uygun bir veri mühendisliği ve veri bilimi pipeline’ı oluşturmaktır.

Proje kapsamında aşağıdaki süreçler gerçekleştirilmiştir:

- MovieLens 25M veri setinin kullanılması
- Kafka ile streaming veri üretimi
- Spark Structured Streaming ile Kafka’dan veri okunması
- Delta Lake üzerinde Bronze, Silver ve Gold veri katmanlarının oluşturulması
- Movie rating verilerinin film bilgileriyle zenginleştirilmesi
- Gold katmanında analitik tabloların üretilmesi
- Spark MLlib ALS algoritması ile öneri/rating tahmin modeli eğitilmesi
- MLflow ile model parametreleri, metrikleri ve artifact takibi
- Streamlit dashboard ile analitik çıktıların görselleştirilmesi

---

## Kullanılan Teknolojiler

| Teknoloji | Kullanım Amacı |
|---|---|
| Docker Compose | Tüm servisleri konteynerize ortamda çalıştırmak |
| Apache Kafka | Rating verilerini streaming veri olarak taşımak |
| Python Kafka Producer | MovieLens rating verilerini Kafka’ya göndermek |
| Apache Spark | Streaming ve batch veri işleme |
| Spark Structured Streaming | Kafka’dan akan veriyi işlemek |
| Delta Lake | Bronze/Silver/Gold veri katmanlarını saklamak |
| Spark MLlib | ALS tabanlı rating tahmin modeli geliştirmek |
| MLflow | Model deneylerini, metrikleri ve parametreleri takip etmek |
| Streamlit | Dashboard arayüzü oluşturmak |
| Plotly | Dashboard grafiklerini oluşturmak |

---

## Veri Seti

Projede MovieLens 25M veri seti kullanılmıştır.

Veri seti bağlantısı:

```text
https://grouplens.org/datasets/movielens/25m/