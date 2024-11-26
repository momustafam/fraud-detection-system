import unittest
import json
import csv
from kafka import KafkaConsumer

from fraud_detection_system.transaction_producer import TransactionProducer
from utils.setup_kafka import create_kafka_topic


class TestFraudDetectionSystem(unittest.TestCase):
    """
    Unit test for the Fraud Detection System class, specifically the TransactionProducer.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up class-level configurations for tests.
        """
        cls.data_path = r".\data\test_transactions.csv"
        cls.kafka_servers = ["127.0.0.1:9092"]
        cls.topic_name = "test_transactions"

    def setUp(self):
        """
        Set up the test environment before each individual test.
        Initializes a TransactionProducer instance.
        """
        self.producer = TransactionProducer(
            self.data_path, self.kafka_servers, self.topic_name
        )

    def test_read_trans(self):
        """
        Test reading transactions from the CSV file.
        Verifies that the number of transactions read matches the expected count.
        """
        transaction_count = 0
        for transaction in self.producer.read_trans():
            transaction_count += 1
        self.assertEqual(transaction_count, 5)  # 5 is the expected transaction count from the provided CSV

    def test_produce_trans(self):
        """
        Test producing transactions to the appropriate Kafka topic.
        Verifies that the transactions produced match those in the CSV file.
        """
        # Create Kafka topic
        create_kafka_topic(self.kafka_servers[0], self.topic_name, 1, 1)
        self.producer.produce_transaction()

        # Set up the Kafka consumer
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        print("Consuming all transactions from Kafka...\n")

        messages = []
        try:
            # Consume messages from Kafka topic
            for message in consumer:
                messages.append(message.value)
                if len(messages) >= 5:
                    consumer.commit()
                    break
        except Exception as e:
            print(f"Error consuming messages: {e}\n")
        finally:
            consumer.close()

        # Compare messages with CSV data
        with open(self.data_path, 'r') as file:
            reader = csv.DictReader(file)
            csv_data = [row for row in reader]

        for message, row in zip(messages, csv_data):
            self.assertEqual(message, row)

if __name__ == "__main__":
    unittest.main()
