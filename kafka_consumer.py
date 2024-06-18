import yaml
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from statistics import mean
import time

# History of calculated averages
previous_averages = []
# previous_average = None

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
        current_avg = mean(readings)
        previous_averages.append(current_avg)
        # previous_average = current_avg
        print(f"All readings are within 2.0 degrees. Average: {current_avg}")
        return current_avg, "Reliable"

# [20, 20, 30, 30]
# 0 10, 0;

    # If only one sensor deviates by 2.0 or more
    if len([d for d in differences if d >= 2.0]) == 1:
        if differences[0] >= 2.0:
            current_avg = mean(readings[1:])
        elif differences[-1] >= 2.0:
            current_avg = mean(readings[:-1])
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

# def clean_data(sensors):
#     readings = sorted(sensors)
#     differences = [abs(readings[i] - readings[i + 1]) for i in range(len(readings) - 1)]
#     outlier_list = [readings[i] for i in range(len(readings) - 1) if abs(readings[i] - readings[i + 1]) > 2]

#     if all(diff < 2.0 for diff in differences):
#         return mean(readings), "Reliable"
#     elif (abs(readings[0] - readings[1]) > 2 and not abs(readings[3] - readings[2]) > 2) \
#         or ((abs(readings[3] - readings[2]) > 2 and not abs(readings[0] - readings[1]) > 2)) :
#         readings = [reading for reading in readings if reading not in outlier_list]
#         return mean(list(readings)), "UnreliableSensorReading"
#     elif (abs(readings[0] - readings[1]) > 2 and abs(readings[3] - readings[2]) > 2) or (abs(readings[2] - readings[1])):
#         return None, "UnreliableRow"

def is_outlier(current_avg):
    # if previous_average:
    #     print(f"Checking if current average {current_avg} is an outlier compared to last average {previous_average}: {is_outlier}")
    #     return abs(current_avg - previous_average) >= 2
    if previous_averages:
        last_avg = previous_averages[-1]
        is_outlier = abs(current_avg - last_avg) >= 2.0
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
                    "reason": "OutlierAverageValue",
                    "data": data
                })
                print(f"Sent outlier average value to monitoring topic: {data}")
            else:
                producer_clean.send(config['kafka']['clean_data_topic'], value={
                    "station_id": data['station_id'],
                    "timestamp": data['timestamp'],
                    "temperature": clean_value
                })
                if status != "Reliable":
                    print(f"Sent clean data to clean_data topic: {data}")
                    producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                        "reason": status,
                        "data": data
                    })
                    print(f"Sent reliable/unreliable sensor reading to monitoring topic: {data}")
        else:
            producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                "reason": status,
                "data": data
            })
            print(f"Sent unreliable row to monitoring topic: {data}")

if __name__ == "__main__":
    config = load_config()
    consumer = create_kafka_consumer(config)
    producer_clean = create_kafka_producer(config)
    producer_monitoring = create_kafka_producer(config)
    
    process_messages(consumer, producer_clean, producer_monitoring, config)
