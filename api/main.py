from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ARRAY, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import os
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel

# Configuration
POSTGRES_DB = os.getenv("POSTGRES_DB", "cryptoviz")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"

# Wait for DB not handled here, assume handled by Docker restart/healthchecks or manually retry connection
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class Article(Base):
    __tablename__ = "articles"
    # id VARCHAR(50) PRIMARY KEY, title TEXT, url TEXT, content TEXT, published_at TIMESTAMP, scraped_at TIMESTAMP, sentiment_score FLOAT, crypto_mentions TEXT[]
    id = Column(String, primary_key=True, index=True)
    title = Column(String)
    url = Column(String)
    content = Column(String)
    published_at = Column(DateTime)
    scraped_at = Column(DateTime)
    sentiment_score = Column(Float)
    crypto_mentions = Column(ARRAY(String)) 

# Pydantic schemas
class ArticleResponse(BaseModel):
    id: str
    title: str
    url: str
    published_at: Optional[datetime]
    sentiment_score: Optional[float]
    crypto_mentions: Optional[List[str]]

    class Config:
        from_attributes = True

app = FastAPI(title="CryptoViz API")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return {"message": "Welcome to CryptoViz API"}

@app.get("/articles", response_model=List[ArticleResponse])
def get_articles(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    articles = db.query(Article).order_by(Article.published_at.desc()).offset(skip).limit(limit).all()
    return articles

@app.get("/stats/sentiment")
def get_sentiment_stats(db: Session = Depends(get_db)):
    # Simple average sentiment
    result = db.execute(text("SELECT AVG(sentiment_score) as avg_sentiment, COUNT(*) as count FROM articles"))
    row = result.fetchone()
    # Handle None if no data
    avg = row[0] if row and row[0] is not None else 0.0
    count = row[1] if row else 0
    return {"average_sentiment": avg, "total_articles": count}

@app.get("/stats/crypto")
def get_crypto_stats(db: Session = Depends(get_db)):
    # Count mentions
    query = """
    SELECT token, COUNT(*) as count, AVG(sentiment_score) as avg_sentiment
    FROM (
        SELECT unnest(crypto_mentions) as token, sentiment_score
        FROM articles
    ) sub
    GROUP BY token
    ORDER BY count DESC
    LIMIT 10
    """
    try:
        result = db.execute(text(query)).fetchall()
        return [{"crypto": row[0], "count": row[1], "avg_sentiment": row[2] if row[2] else 0.0} for row in result]
    except Exception as e:
        # If table doesn't exist or empty
        return []

