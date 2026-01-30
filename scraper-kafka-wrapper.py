#!/usr/bin/env python3
"""
CryptoViz - Kafka Producer Wrapper for Toyz Scraper
Captures JSON output from Go scraper and publishes to Kafka
"""

import subprocess
import json
import sys
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import hashlib
import re

class ScraperKafkaWrapper:
    def __init__(self, kafka_brokers, kafka_topic):
        """Initialize Kafka producer"""
        self.kafka_topic = kafka_topic
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers.split(','),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8'),
            acks='all',
            retries=3,
            compression_type='gzip',
            linger_ms=10,
            batch_size=16384
        )
        
        print(f"INFO: Kafka producer initialized (brokers: {kafka_brokers})")
        print(f"INFO: Publishing to topic: {kafka_topic}")
    
    def generate_article_id(self, title, url):
        """Generate unique ID for article"""
        unique_string = f"{title}-{url}"
        return hashlib.md5(unique_string.encode()).hexdigest()[:16]
    
    def extract_crypto_mentions(self, text):
        """Extract cryptocurrency mentions from text"""
        if not text:
            return []
        
        # Common crypto keywords
        crypto_patterns = [
            r'\bBTC\b', r'\bBitcoin\b',
            r'\bETH\b', r'\bEthereum\b',
            r'\bSOL\b', r'\bSolana\b',
            r'\bBNB\b', r'\bBinance\b',
            r'\bADA\b', r'\bCardano\b',
            r'\bDOGE\b', r'\bDogecoin\b',
            r'\bXRP\b', r'\bRipple\b',
            r'\bMATIC\b', r'\bPolygon\b',
            r'\bDOT\b', r'\bPolkadot\b',
            r'\bLINK\b', r'\bChainlink\b',
            r'\bAVAX\b', r'\bAvalanche\b',
            r'\bSHIB\b', r'\bShiba Inu\b',
            r'\bOP\b', r'\bOptimism\b',
        ]
        
        mentions = set()
        text_upper = text.upper()
        
        for pattern in crypto_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                # Extract the crypto name
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    mentions.add(match.group(0))
        
        return list(mentions)
    
    def parse_article_from_json(self, json_line):
        """Parse article from JSON output"""
        try:
            article = json.loads(json_line)
            
            # Generate ID if not present
            if 'id' not in article or not article['id']:
                article['id'] = self.generate_article_id(
                    article.get('title', ''),
                    article.get('url', '')
                )
            
            # Add scraped_at timestamp
            article['scraped_at'] = datetime.now().isoformat()
            
            # Extract crypto mentions from title and content
            title = article.get('title', '')
            content = article.get('content', '')
            crypto_mentions = self.extract_crypto_mentions(f"{title} {content}")
            
            if crypto_mentions:
                article['crypto_mentions'] = crypto_mentions
            
            # Ensure published_at is in ISO format
            if 'published_at' in article:
                # Try to parse and convert to ISO format
                try:
                    if isinstance(article['published_at'], str):
                        # Already a string, keep as is
                        pass
                    else:
                        article['published_at'] = str(article['published_at'])
                except:
                    article['published_at'] = datetime.now().isoformat()
            else:
                article['published_at'] = datetime.now().isoformat()
            
            return article
            
        except json.JSONDecodeError as e:
            print(f"WARN: Failed to parse JSON: {e}")
            return None
    
    def publish_article(self, article):
        """Publish article to Kafka"""
        try:
            future = self.producer.send(
                self.kafka_topic,
                key=article['id'],
                value=article
            )
            
            # Wait for confirmation (optional, can be async)
            record_metadata = future.get(timeout=10)
            
            print(f"INFO: Published: {article.get('title', 'Unknown')} "
                  f"[partition {record_metadata.partition}, offset {record_metadata.offset}]")
            
            return True
            
        except KafkaError as e:
            print(f"ERROR: Kafka error: {e}")
            return False
        except Exception as e:
            print(f"ERROR: Error publishing article: {e}")
            return False
    
    def run_scraper_and_publish(self, scraper_command):
        """Run Go scraper and publish articles to Kafka"""
        print(f"INFO: Starting scraper: {' '.join(scraper_command)}")
        print("=" * 60)
        
        articles_published = 0
        
        try:
            # Start the scraper process
            process = subprocess.Popen(
                scraper_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            # Read output line by line
            for line in process.stdout:
                line = line.strip()
                
                # Print the original line
                print(f"[SCRAPER] {line}")
                
                # Try to parse as JSON
                if line.startswith('{') and line.endswith('}'):
                    article = self.parse_article_from_json(line)
                    
                    if article:
                        if self.publish_article(article):
                            articles_published += 1
            
            # Wait for process to complete
            process.wait()
            
            # Flush remaining messages
            print("\nINFO: Flushing remaining messages...")
            self.producer.flush(timeout=15)
            
            print("=" * 60)
            print(f"INFO: Scraping complete!")
            print(f"INFO: Total articles published: {articles_published}")
            
            return process.returncode
            
        except KeyboardInterrupt:
            print("\nWARN: Interrupted by user")
            process.terminate()
            self.producer.flush()
            return 1
        
        except Exception as e:
            print(f"ERROR: Error: {e}")
            return 1
    
    def close(self):
        """Close Kafka producer"""
        self.producer.close()
        print("INFO: Kafka producer closed")


def main():
    # Configuration from environment variables
    kafka_brokers = os.getenv('KAFKA_BROKERS', 'localhost:9094')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'crypto-news-raw')
    
    # Scraper command
    scraper_cmd = os.getenv('SCRAPER_CMD', './toyz --until 2024-01-01T00:00:00Z')
    scraper_command = scraper_cmd.split()
    
    print("INFO: Configuration:")
    print(f"   Kafka Brokers: {kafka_brokers}")
    print(f"   Kafka Topic: {kafka_topic}")
    print(f"   Scraper Command: {scraper_cmd}")
    print()
    
    # Initialize wrapper
    wrapper = ScraperKafkaWrapper(kafka_brokers, kafka_topic)
    
    try:
        # Run scraper and publish articles
        exit_code = wrapper.run_scraper_and_publish(scraper_command)
        sys.exit(exit_code)
        
    finally:
        wrapper.close()


if __name__ == '__main__':
    main()