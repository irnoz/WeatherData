from kafka import KafkaConsumer
import json

def consume_topic(topic, config):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
    )

    print(f"Consuming messages from topic: {topic}")
    for message in consumer:
        print(f"Key: {message.key}, Value: {message.value}")

if __name__ == "__main__":
    config = {
        'kafka': {
            'bootstrap_servers': 'localhost:9092',
            'clean_data_topic': 'clean_data',
            'monitoring_topic': 'monitoring'
        }
    }
    
    consume_topic(config['kafka']['clean_data_topic'], config)
    consume_topic(config['kafka']['monitoring_topic'], config)
