FROM python:3.9-slim-bookworm

# Install OpenJDK 17 (required for Spark) and libpq-dev for psycopg2
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless libpq-dev gcc && \
    apt-get clean;

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download TextBlob corpora/models
RUN python -m textblob.download_corpora

COPY processor.py .

CMD ["python", "processor.py"]
