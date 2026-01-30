from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time

def test_kafka_connection():
    broker = 'localhost:9094'
    topic_name = 'test-topic'
    
    print(f"🔄 Connecting to Kafka at {broker}...")
    
    # 1. Test Admin Client & Create Topic
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        print("✅ Admin Client connected")
        
        # Check if topic exists
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            print(f"Creating topic {topic_name}...")
            topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"✅ Topic {topic_name} created")
        else:
            print(f"✅ Topic {topic_name} exists")
            
    except Exception as e:
        print(f"❌ Error connecting to Kafka Admin: {e}")
        return

    # 2. Test Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        message = {'status': 'ok', 'timestamp': time.time()}
        producer.send(topic_name, message)
        producer.flush()
        print("✅ Producer sent message successfully")
    except Exception as e:
        print(f"❌ Error producing message: {e}")
        return

    # 3. Test Consumer
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=broker,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='test-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  # Stop after 5 seconds if no message
        )
        
        print("Waiting for message...")
        for message in consumer:
            print(f"✅ Consumer received: {message.value}")
            break
        consumer.close()
        
    except Exception as e:
        print(f"❌ Error consuming message: {e}")

if __name__ == "__main__":
    test_kafka_connection()
