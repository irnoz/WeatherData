import yaml
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from statistics import mean
import time

previous_averages = []

def load_config():
    with open("config/config.yaml", 'r') as ymlfile:
        return yaml.safe_load(ymlfile)

def create_kafka_consumer(config):
    while True:
        try:
            consumer = KafkaConsumer(
                config['kafka']['valid_data_topic'],
                bootstrap_servers=config['kafka']['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Kafka consumer created successfully.")
            return consumer
        except NoBrokersAvailable:
            print("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)

def create_kafka_producer(config):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config['kafka']['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Kafka producer created successfully.")
            return producer
        except NoBrokersAvailable:
            print("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)

def clean_data(sensors):
    print(f"Cleaning data for sensors: {sensors}")
    readings = sorted(sensors)
    differences = [abs(readings[i] - readings[i + 1]) for i in range(len(readings) - 1)]
    print(f"Sorted readings: {readings}")
    print(f"Differences between readings: {differences}")

    # If all readings are within 2.0 degrees
    if all(diff < 2.0 for diff in differences):
        current_avg = round(mean(readings), 3)
        previous_averages.append(current_avg)
        # previous_average = current_avg
        print(f"All readings are within 2.0 degrees. Average: {current_avg}")
        return current_avg, "Reliable"

    # If only one sensor deviates by 2.0 or more
    if len([d for d in differences if d >= 2.0]) == 1:
        if differences[0] >= 2.0:
            current_avg = round(mean(readings[1:]), 3)
        elif differences[-1] >= 2.0:
            current_avg = round(mean(readings[:-1]), 3)
        else:
            return None, "UnreliableRow"
        previous_averages.append(current_avg)
        # previous_average = current_avg
        print(f"One sensor deviates by 2.0 or more. Average: {current_avg}")
        return current_avg, "UnreliableSensorReading"

    # If there are two deviating sensors or clusters
    if len([d for d in differences if d >= 2.0]) > 1:
        print("Two sensors deviate or form clusters. Unreliable row.")
        return None, "UnreliableRow"

    print("Unreliable row detected.")
    return None, "UnreliableRow"

def is_outlier(current_avg):
    if len(previous_averages) > 1:
        last_avg = previous_averages[-2]
        is_outlier = abs(current_avg - last_avg) >= 2.0
        if is_outlier:
            previous_averages.pop()
        print(f"Checking if current average {current_avg} is an outlier compared to last average {last_avg}: {is_outlier}")
        return is_outlier
    return False

def process_messages(consumer, producer_clean, producer_monitoring, config):
    for message in consumer:
        data = message.value
        print("-------------------------------------New Entry-------------------------------------")
        print(f"Received message: {data}")
        sensors = [data['sensor0'], data['sensor1'], data['sensor2'], data['sensor3']]
        clean_value, status = clean_data(sensors)

        if clean_value is not None:
            print("Previous Averages: ",previous_averages)
            if is_outlier(clean_value):
                producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                    "err_type": "OutlierAverageValue",
                    "ts": data['ts'],
                    "sensor_id": None,
                    "station_id": data['station_id'],
                })
                print(f"Sent outlier average value to monitoring topic: {data}")
            else:
                producer_clean.send(config['kafka']['clean_data_topic'], value={
                    "ts": data['ts'],
                    # "sensor_id": data['sensor_id'],
                    "station_id": data['station_id'],
                    "temperature": clean_value
                })
                print(f"Sent reliable sensor reading to clean_data topic: {data}")
                if status != "Reliable":
                    print(f"Sent clean data to clean_data topic: {data}")
                    producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                        "err_type": status,
                        "ts": data['ts'],
                        "sensor_id": None,
                        "station_id": data['station_id'],
                    })
                    print(f"Sent unreliable sensor reading to monitoring topic: {data}")
        else:
            producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                "err_type": status,
                "ts": data['ts'],
                "sensor_id": None,
                "station_id": data['station_id'],
            })
            print(f"Sent unreliable row to monitoring topic: {data}")

if __name__ == "__main__":
    config = load_config()
    consumer = create_kafka_consumer(config)
    producer_clean = create_kafka_producer(config)
    producer_monitoring = create_kafka_producer(config)
    
    process_messages(consumer, producer_clean, producer_monitoring, config)
