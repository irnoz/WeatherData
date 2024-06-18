import unittest
from unittest.mock import MagicMock, patch
from kafka_consumer import load_config, create_kafka_consumer, create_kafka_producer, clean_data, process_messages, previous_averages

class TestKafkaConsumer(unittest.TestCase):

    def setUp(self):
        self.config = {
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'valid_data_topic': 'valid_data',
                'clean_data_topic': 'clean_data',
                'monitoring_topic': 'monitoring'
            }
        }
        # Reset the previous_averages before each test
        global previous_averages
        previous_averages = []

    def test_load_config(self):
        config = load_config()
        self.assertIn('kafka', config)
        self.assertIn('bootstrap_servers', config['kafka'])

    @patch('kafka_consumer.KafkaConsumer')
    def test_create_kafka_consumer(self, MockKafkaConsumer):
        MockKafkaConsumer.return_value = MagicMock()
        consumer = create_kafka_consumer(self.config)
        self.assertIsInstance(consumer, MagicMock)

    @patch('kafka_consumer.KafkaProducer')
    def test_create_kafka_producer(self, MockKafkaProducer):
        MockKafkaProducer.return_value = MagicMock()
        producer = create_kafka_producer(self.config)
        self.assertIsInstance(producer, MagicMock)

    def test_clean_data_reliable(self):
        sensors = [20.5, 20.7, 20.6, 20.8]
        clean_value, status = clean_data(sensors)
        self.assertEqual(clean_value, 20.65)
        self.assertEqual(status, "Reliable")

    def test_clean_data_unreliable_sensor(self):
        sensors = [3.5, 4.2, 4.0, 6.4]
        clean_value, status = clean_data(sensors)
        self.assertEqual(clean_value, 3.9)
        self.assertEqual(status, "UnreliableSensorReading")

    def test_clean_data_unreliable_row(self):
        sensors = [3.4, 5.6, 6.0, 8.2]
        clean_value, status = clean_data(sensors)
        self.assertIsNone(clean_value)
        self.assertEqual(status, "UnreliableRow")

    def test_clean_data_unreliable_row_cluster(self):
        sensors = [4.5, 4.6, 7.5, 7.6]
        clean_value, status = clean_data(sensors)
        self.assertIsNone(clean_value)
        self.assertEqual(status, "UnreliableRow")

    @patch('kafka_consumer.create_kafka_consumer')
    @patch('kafka_consumer.create_kafka_producer')
    @patch('kafka_consumer.clean_data')
    def test_process_messages(self, mock_clean_data, mock_create_producer, mock_create_consumer):
        mock_consumer = MagicMock()
        mock_consumer.__iter__.return_value = [
            MagicMock(value={
                'station_id': 'station1',
                'timestamp': '2023-06-16T00:00:00Z',
                'sensor0': 20.5,
                'sensor1': 20.7,
                'sensor2': 20.6,
                'sensor3': 20.8
            })
        ]
        mock_create_consumer.return_value = mock_consumer
        mock_producer = MagicMock()
        mock_create_producer.return_value = mock_producer
        mock_clean_data.return_value = (20.65, "Reliable")

        process_messages(mock_consumer, mock_producer, mock_producer, self.config)

        mock_producer.send.assert_any_call(
            'clean_data',
            value={
                'station_id': 'station1',
                'timestamp': '2023-06-16T00:00:00Z',
                'temperature': 20.65
            }
        )
        mock_producer.send.assert_any_call(
            'monitoring',
            value={
                'reason': 'Reliable',
                'data': {
                    'station_id': 'station1',
                    'timestamp': '2023-06-16T00:00:00Z',
                    'sensor0': 20.5,
                    'sensor1': 20.7,
                    'sensor2': 20.6,
                    'sensor3': 20.8
                }
            }
        )

if __name__ == '__main__':
    unittest.main()
