# CryptoViz - Sentiment Analysis Pipeline

This project is a complete real-time data pipeline that scrapes cryptocurrency news, performs sentiment analysis, and serves the data via an API.

## Architecture

The system is composed of several microservices orchestrated by Docker Compose:

1.  **Scraper (Go + Python Wrapper)**
    -   **Core (Go)**: A high-performance binary (`toyz`) responsible for crawling CoinMarketCap and CoinDesk articles. It outputs raw data as JSON to stdout.
    -   **Wrapper (Python)**: A script that executes the Go binary, captures its output, processes the JSON (adding timestamps, IDs), and produces messages to Kafka.
    -   **Why this hybrid approach?**: Go offers concurrency and speed for scraping, while Python provides rich libraries for Kafka integration and text processing.

2.  **Message Broker (Kafka + Zookeeper)**
    -   Acts as a buffer between the scraper and the analytics engine.
    -   Decouples data ingestion from data processing.
    -   Topic: `crypto-news-raw`

3.  **Analytics Engine (PySpark)**
    -   Consumes messages from the Kafka topic.
    -   Performs sentiment analysis using `TextBlob` (generating a score from -1.0 to 1.0).
    -   Extracts cryptocurrency mentions.
    -   Writes processed data to PostgreSQL.

4.  **Database (PostgreSQL)**
    -   Stores the structured articles with their sentiment scores and metadata.

5.  **API (FastAPI)**
    -   Provides a RESTful interface to query the analyzed data.
    -   Endpoint: `/articles`

## Prerequisites

-   Docker
-   Docker Compose

## Installation & Usage

1.  **Start the services**:
    ```bash
    docker compose up --build -d
    ```

    This will start Zookeeper, Kafka, Postgres, the API, the Analytics engine, and the Scraper.

2.  **Monitor the Scraper**:
    To see the articles being scraped and sent to Kafka:
    ```bash
    docker logs -f cryptoviz-scraper
    ```

3.  **Monitor Analytics**:
    To see Spark processing batches and writing to the DB:
    ```bash
    docker logs -f cryptoviz-analytics
    ```

4.  **Access the API**:
    The API runs on port `8001`.
    -   List articles: [http://localhost:8001/articles](http://localhost:8001/articles)
    -   Documentation: [http://localhost:8001/docs](http://localhost:8001/docs)

## Component Details

### Scraper Service (`/toyz`)
-   **Dockerfile**: Multi-stage build. First compiles the Go binary, then copies it into a Python-based image for the wrapper.
-   **Behavior**: Scrapes articles historically from the current date backwards until a specified cutoff (defaults to 2026-01-29 in `docker-compose.yml`).

### Analytics Service (`/analytics`)
-   Uses Apache Spark Structured Streaming.
-   Reads from Kafka.
-   Applies `udf` (User Defined Function) for sentiment analysis.
-   Writes to Postgres using JDBC.

### API Service (`/api`)
-   Built with FastAPI and SQLAlchemy.
-   Exposes `Article` model.

## Troubleshooting

-   **Empty API Response**: Ensure the scraper is running and has not timed out. Check `docker logs cryptoviz-scraper`.
-   **Postgres Connection Errors**: Ensure the `postgres` service is healthy before other services try to connect. The `depends_on` condition in `docker-compose.yml` handles this.
-   **Kafka Issues**: If topics are missing, the scraper might fail to publish. The setup uses `confluentinc/cp-kafka` which should auto-create topics if configured, or the producer will create them if `auto.create.topics.enable` is true (default).
