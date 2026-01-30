# Multi-stage Dockerfile for Toyz Scraper + Kafka Producer

FROM golang:1.25-alpine AS go-builder

WORKDIR /crawl

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

# Copy go mod files
COPY go.mod go.sum ./

# Utilise le proxy Go direct pour éviter les problèmes réseau
ENV GOPROXY=direct
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o toyz .

# Stage 2: Runtime with Python
FROM python:3.11-slim

WORKDIR /crawl

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy Go binary from builder
COPY --from=go-builder /crawl/toyz ./toyz

# Make it executable
RUN chmod +x ./toyz

# Copy Python wrapper and requirements
COPY scraper-kafka-wrapper.py ./
COPY scraper-requirements.txt ./requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Environment variables (can be overridden)
ENV KAFKA_BROKERS=kafka:9092
ENV KAFKA_TOPIC=crypto-news-raw
ENV SCRAPER_CMD="./toyz --until 2024-01-01T00:00:00Z"

# Run the Python wrapper which launches the Go scraper
CMD ["python", "scraper-kafka-wrapper.py"]