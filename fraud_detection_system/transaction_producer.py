"""
A simple module with a class that acts as a Kafka producer for transaction data.
"""

import csv
import json
from typing import Iterator, List
from kafka import KafkaProducer
from time import sleep

class TransactionProducer:
    """
    A class that acts as a Kafka producer for producing transaction data.

    Attributes:
        data_path (str): The file path to the CSV file containing transaction data.
        kafka_servers (list): List of Kafka server addresses.
        topic_name (str): Kafka topic to which the transactions will be sent.

    Methods:
        read_trans(self) -> Iterator[dict]:
            Reads transactions one at a time from a CSV file.

        produce_transaction(self) -> None:
            Produces transactions to a Kafka topic.
    """

    def __init__(self, data_path: str, kafka_servers: List[str], topic_name: str) -> None:
        """
        Initializes the transaction producer with the given data path, Kafka server addresses,
        and topic name.

        Args:
            data_path (str): The path to the CSV file containing transaction data.
            kafka_servers (List[str]): A list of Kafka server addresses.
            topic_name (str): The Kafka topic to send the transactions to.
        """
        self.data_path = data_path
        self.kafka_servers = kafka_servers
        self.topic_name = topic_name

    def read_trans(self) -> Iterator[dict]:
        """
        Reads transactions one row at a time from a CSV file.

        Yields:
            dict: A dictionary representing a transaction, where the keys are column names and the values are row data.

        Raises:
            FileNotFoundError: If the specified file cannot be found.
            Exception: If there is an error while reading the file.
        """
        try:
            with open(self.data_path, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    yield row  # Return one row at a time as a dictionary
        except FileNotFoundError:
            print(f"Error: File not found at {self.data_path}")
        except Exception as e:
            print(f"Error reading file: {e}")

    def produce_transaction(self) -> None:
        """
        Produces transactions to a Kafka topic. This method reads transactions from the
        CSV file and sends them to the Kafka topic for further processing.

        This method uses the KafkaProducer to send each transaction to Kafka and confirms
        successful delivery.

        Raises:
            Exception: If an error occurs during the sending of the transaction to Kafka.
        """
        
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda trans: json.dumps(trans).encode('utf-8'),
        )
    

        for trans in self.read_trans():
            try:
                # Send the message to the Kafka topic
                req = producer.send(self.topic_name, trans)
                # Confirm successful delivery
                record_metadata = req.get(timeout=10)
                print(
                    f"Transaction sent to topic '{record_metadata.topic}' "
                    f"at partition {record_metadata.partition}, offset {record_metadata.offset}"
                )
            except Exception as e:
                print(f"Failed to send transaction: {e}\n")
            sleep(1)
        print("\n\n\n")

        producer.close()