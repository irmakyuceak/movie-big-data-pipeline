import os
import pandas as pd
import streamlit as st
import plotly.express as px


BASE_DELTA_PATH = "/app/delta/gold"

MOST_RATED_PATH = os.path.join(BASE_DELTA_PATH, "most_rated_movies")
TOP_RATED_PATH = os.path.join(BASE_DELTA_PATH, "top_rated_movies")
GENRE_STATS_PATH = os.path.join(BASE_DELTA_PATH, "genre_rating_stats")
USER_ACTIVITY_PATH = os.path.join(BASE_DELTA_PATH, "user_activity_stats")
ML_DATASET_PATH = os.path.join(BASE_DELTA_PATH, "ml_ratings_dataset")


st.set_page_config(
    page_title="MovieLens Big Data Dashboard",
    page_icon="🎬",
    layout="wide"
)


@st.cache_data
def read_delta_as_pandas(path: str) -> pd.DataFrame:
    """
    Delta klasöründeki parquet dosyalarını pandas dataframe olarak okur.
    _delta_log klasörünü doğrudan kullanmıyoruz; dashboard için final parquet çıktıları yeterli.
    """
    if not os.path.exists(path):
        return pd.DataFrame()

    parquet_files = [
        os.path.join(path, file)
        for file in os.listdir(path)
        if file.endswith(".parquet")
    ]

    if not parquet_files:
        return pd.DataFrame()

    return pd.concat(
        [pd.read_parquet(file) for file in parquet_files],
        ignore_index=True
    )


def show_missing_data_warning():
    st.error(
        "Gold katmanı bulunamadı veya boş görünüyor. "
        "Önce Spark tarafında silver_to_gold.py script'ini çalıştırmalısın."
    )


most_rated_df = read_delta_as_pandas(MOST_RATED_PATH)
top_rated_df = read_delta_as_pandas(TOP_RATED_PATH)
genre_stats_df = read_delta_as_pandas(GENRE_STATS_PATH)
user_activity_df = read_delta_as_pandas(USER_ACTIVITY_PATH)
ml_dataset_df = read_delta_as_pandas(ML_DATASET_PATH)


st.title("🎬 MovieLens Big Data Pipeline Dashboard")

st.markdown(
    """
Bu dashboard, MovieLens 25M veri seti üzerinde kurulan büyük veri pipeline'ının 
**Gold katmanındaki analitik çıktılarını** göstermektedir.

Pipeline akışı:

`Kafka Producer → Apache Kafka → Spark Structured Streaming → Delta Bronze → Delta Silver → Delta Gold → MLflow`
"""
)

if (
    most_rated_df.empty
    or top_rated_df.empty
    or genre_stats_df.empty
    or user_activity_df.empty
    or ml_dataset_df.empty
):
    show_missing_data_warning()
    st.stop()


# ----------------------------
# KPI Alanı
# ----------------------------

total_ratings = len(ml_dataset_df)
total_movies = ml_dataset_df["movieId"].nunique()
total_users = ml_dataset_df["userId"].nunique()
avg_rating = round(ml_dataset_df["rating"].mean(), 2)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Toplam Rating", f"{total_ratings:,}")

with col2:
    st.metric("Tekil Film", f"{total_movies:,}")

with col3:
    st.metric("Tekil Kullanıcı", f"{total_users:,}")

with col4:
    st.metric("Ortalama Rating", avg_rating)


st.divider()


# ----------------------------
# Sidebar filtreleri
# ----------------------------

st.sidebar.header("Filtreler")

top_n = st.sidebar.slider(
    "Grafiklerde gösterilecek kayıt sayısı",
    min_value=5,
    max_value=30,
    value=15,
    step=5
)

min_rating_count = st.sidebar.slider(
    "Top rated filmler için minimum rating sayısı",
    min_value=10,
    max_value=200,
    value=30,
    step=10
)


# ----------------------------
# En Çok Puanlanan Filmler
# ----------------------------

st.header("1. En Çok Puanlanan Filmler")

most_rated_view = most_rated_df.head(top_n).sort_values("rating_count", ascending=True)

fig_most_rated = px.bar(
    most_rated_view,
    x="rating_count",
    y="title",
    orientation="h",
    hover_data=["avg_rating", "genres"],
    title=f"En Çok Puanlanan İlk {top_n} Film"
)

st.plotly_chart(fig_most_rated, use_container_width=True)

with st.expander("Tabloyu göster"):
    st.dataframe(
        most_rated_df.head(50),
        use_container_width=True
    )


# ----------------------------
# En Yüksek Ortalama Rating Alan Filmler
# ----------------------------

st.header("2. Ortalama Rating'e Göre En Başarılı Filmler")

filtered_top_rated_df = top_rated_df[
    top_rated_df["rating_count"] >= min_rating_count
].head(top_n)

top_rated_view = filtered_top_rated_df.sort_values("avg_rating", ascending=True)

fig_top_rated = px.bar(
    top_rated_view,
    x="avg_rating",
    y="title",
    orientation="h",
    hover_data=["rating_count", "genres"],
    title=f"En Yüksek Ortalama Rating Alan İlk {top_n} Film"
)

st.plotly_chart(fig_top_rated, use_container_width=True)

with st.expander("Tabloyu göster"):
    st.dataframe(
        filtered_top_rated_df,
        use_container_width=True
    )


# ----------------------------
# Tür Bazlı Analiz
# ----------------------------

st.header("3. Tür Bazlı Rating Analizi")

genre_col1, genre_col2 = st.columns(2)

with genre_col1:
    genre_count_view = genre_stats_df.sort_values("rating_count", ascending=False)

    fig_genre_count = px.bar(
        genre_count_view,
        x="genre",
        y="rating_count",
        title="Türlere Göre Rating Sayısı"
    )

    st.plotly_chart(fig_genre_count, use_container_width=True)

with genre_col2:
    genre_avg_view = genre_stats_df.sort_values("avg_rating", ascending=False)

    fig_genre_avg = px.bar(
        genre_avg_view,
        x="genre",
        y="avg_rating",
        title="Türlere Göre Ortalama Rating"
    )

    st.plotly_chart(fig_genre_avg, use_container_width=True)

with st.expander("Tür istatistikleri tablosu"):
    st.dataframe(
        genre_stats_df,
        use_container_width=True
    )


# ----------------------------
# Kullanıcı Aktivitesi
# ----------------------------

st.header("4. En Aktif Kullanıcılar")

user_activity_view = user_activity_df.head(top_n).sort_values("rating_count", ascending=True)

fig_user_activity = px.bar(
    user_activity_view,
    x="rating_count",
    y="userId",
    orientation="h",
    hover_data=["avg_user_rating"],
    title=f"En Çok Rating Veren İlk {top_n} Kullanıcı"
)

st.plotly_chart(fig_user_activity, use_container_width=True)

with st.expander("Kullanıcı aktivite tablosu"):
    st.dataframe(
        user_activity_df.head(100),
        use_container_width=True
    )


# ----------------------------
# ML Dataset Özeti
# ----------------------------

st.header("5. Makine Öğrenmesi Veri Seti Özeti")

ml_col1, ml_col2 = st.columns(2)

with ml_col1:
    rating_dist = (
        ml_dataset_df
        .groupby("rating")
        .size()
        .reset_index(name="count")
        .sort_values("rating")
    )

    fig_rating_dist = px.bar(
        rating_dist,
        x="rating",
        y="count",
        title="Rating Dağılımı"
    )

    st.plotly_chart(fig_rating_dist, use_container_width=True)

with ml_col2:
    st.markdown("### MLflow Model Sonucu")

    st.info(
        """
Modelleme aşamasında Spark MLlib ALS algoritması kullanılmıştır.

MLflow üzerinde takip edilen örnek sonuçlar:

- Model: ALS
- Dataset: gold_ml_ratings_dataset
- Rank: 10
- MaxIter: 10
- RegParam: 0.1
- RMSE: 0.9272
- MAE: 0.7145
"""
    )

st.divider()

st.caption(
    "MovieLens Big Data Pipeline | Kafka + Spark + Delta Lake + MLflow + Streamlit"
)