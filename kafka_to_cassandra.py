import json
import time
import yaml
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

def load_config():
    with open("config/config.yaml", 'r') as ymlfile:
        return yaml.safe_load(ymlfile)

def create_kafka_consumer(config):
    while True:
        try:
            consumer = KafkaConsumer(
                config['kafka']['clean_data_topic'],
                bootstrap_servers=config['kafka']['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Kafka consumer created successfully.")
            return consumer
        except Exception as e:
            print(f"Error creating Kafka consumer: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_cassandra_session(config):
    while True:
        try:
            auth_provider = PlainTextAuthProvider(
                username=config['cassandra']['username'],
                password=config['cassandra']['password']
            )
            cluster = Cluster([config['cassandra']['host']], auth_provider=auth_provider)
            session = cluster.connect(config['cassandra']['keyspace'])
            print("Cassandra session created successfully.")
            return session
        except Exception as e:
            print(f"Error creating Cassandra session: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def write_to_cassandra(session, data):
    try:
        query = SimpleStatement("""
            INSERT INTO clean_data (station_id, timestamp, temperature)
            VALUES (%s, %s, %s)
        """)
        session.execute(query, (data['station_id'], data['timestamp'], data['temperature']))
        print(f"Data inserted into Cassandra: {data}")
    except Exception as e:
        print(f"Error inserting data into Cassandra: {e}")

def process_messages(consumer, session):
    for message in consumer:
        data = message.value
        print(f"Received message: {data}")
        write_to_cassandra(session, data)

if __name__ == "__main__":
    config = load_config()
    consumer = create_kafka_consumer(config)
    session = create_cassandra_session(config)
    
    process_messages(consumer, session)
