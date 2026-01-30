from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, current_timestamp, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, TimestampType
from textblob import TextBlob
import psycopg2
import os

# --- Configuration ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-news-raw")
POSTGRES_DB = os.getenv("POSTGRES_DB", "cryptoviz")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")

# --- Sentiment Analysis UDF ---
def get_sentiment(text):
    if not text:
        return 0.0
    return TextBlob(text).sentiment.polarity

sentiment_udf = udf(get_sentiment, FloatType())

# --- Postgres Writer ---
def write_to_postgres(df, epoch_id):
    # This runs on the driver, but we need to collect data or map partitions to write
    # For simplicity/robustness in PySpark, usually better to use mapPartitions or foreach
    # But foreachBatch receives a DataFrame, so we can treat it like a batch df
    
    # We'll use simple JDBC or python psycopg2 based on size.
    # For "Big Data" use JDBC writer. For this scale, JDBC is fine.
    
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    # Write raw/processed articles
    mode = "append"
    
    # Ensure tables exist (Hack: Doing this inside the batch loop isn't ideal but simplest for auto-init)
    # Ideally, run an init script.
    
    try:
        df.write.jdbc(url=jdbc_url, table="articles", mode=mode, properties=properties)
        print(f"Batch {epoch_id}: Written {df.count()} records to Postgres.")
    except Exception as e:
        print(f"Error writing batch {epoch_id} to Postgres: {e}")

def init_db():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id VARCHAR(50) PRIMARY KEY,
                title TEXT,
                url TEXT,
                content TEXT,
                published_at TIMESTAMP,
                scraped_at TIMESTAMP,
                sentiment_score FLOAT,
                crypto_mentions TEXT[]
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("INFO: Database initialized")
    except Exception as e:
        print(f"ERROR: Database initialization failed: {e}")

# --- Main ---
def main():
    print("INFO: Starting Spark Processor...")
    
    # Initialize DB
    init_db()

    # Initialize Spark Session
    # We need the Kafka and Postgres JDBC jars
    # In a real environment, you'd mount these or download them.
    # For this Docker setup, we'll try to use ivy packages.
    spark = SparkSession.builder \
        .appName("CryptoVizAnalytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define Schema matching scraper output
    # jsonArticle format: url, title, content, assets, created_at, id, scraped_at, crypto_mentions
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True),
        StructField("content", StringType(), True),
        StructField("created_at", StringType(), True), # Scraper sends string
        StructField("scraped_at", StringType(), True),
        StructField("crypto_mentions", ArrayType(StringType()), True)
    ])

    # Read Stream from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Apply Sentiment Analysis
    df_enriched = df_parsed.withColumn("sentiment_score", sentiment_udf(col("content"))) \
                           .withColumn("published_at", col("created_at").cast(TimestampType())) \
                           .withColumn("scraped_at", col("scraped_at").cast(TimestampType()))

    # Select columns for DB
    df_final = df_enriched.select(
        "id", "title", "url", "content", "published_at", "scraped_at", "sentiment_score", "crypto_mentions"
    )

    # Output to Console for debugging
    query_console = df_final.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Output to Postgres
    query_db = df_final.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()

    query_console.awaitTermination()
    query_db.awaitTermination()

if __name__ == "__main__":
    main()
