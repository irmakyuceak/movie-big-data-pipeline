import os
import json
import time
import pandas as pd
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "movie-ratings")

RATINGS_PATH = "/app/data/raw/ml-25m/ratings.csv"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
    )


def main():
    print("Kafka producer başlatılıyor...")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Dataset path: {RATINGS_PATH}")

    producer = create_producer()

    chunk_size = 1000

    for chunk in pd.read_csv(RATINGS_PATH, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            event = {
                "userId": int(row["userId"]),
                "movieId": int(row["movieId"]),
                "rating": float(row["rating"]),
                "timestamp": int(row["timestamp"]),
            }

            producer.send(
                KAFKA_TOPIC,
                key=event["userId"],
                value=event
            )

        producer.flush()
        print(f"{chunk_size} rating Kafka'ya gönderildi.")
        time.sleep(1)


if __name__ == "__main__":
    main()