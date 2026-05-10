FROM python:3.11-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming_to_delta.py .
COPY train_model.py .
COPY check_bronze.py .
COPY bronze_to_silver.py .
COPY silver_to_gold.py .
COPY feature_engineering.py .
COPY train_regression_models.py .

CMD ["tail", "-f", "/dev/null"]