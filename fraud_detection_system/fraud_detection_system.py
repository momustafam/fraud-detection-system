"""
A simple module that acts as a fraud detector.
"""

import json
import joblib
import numpy as np
import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
from typing import Dict, Any


class FraudDetectionSystem:
    """
    A class that acts as a fraud detector. It consumes transactions from Kafka,
    classifies them, and detects whether they are fraudulent.
    """
    
    def __init__(self, db_config: Dict[str, Any], kafka_servers: list, 
                 topic_name: str, model_path: str) -> None:
        """
        Initializes the fraud detection system with database configuration, 
        Kafka server details, topic name, and the model path.

        Args:
            db_config (dict): The configuration for the MySQL database connection.
            kafka_servers (list): List of Kafka server addresses.
            topic_name (str): Kafka topic name for transaction data.
            model_path (str): Path to the pre-trained model file.
        """
        self.kafka_servers = kafka_servers
        self.topic_name = topic_name        
        self.db_config = db_config
        
        # Load the pre-trained model
        self.model = joblib.load(model_path)

        
    def save_to_database(self, transaction_id: int, 
                         prediction_result: int, 
                         topic_name: str) -> None:
        """Saves the prediction result into the MySQL database.

        Args:
            transaction_id (int): The ID of the transaction.
            prediction_result (int): The prediction result ("0" for non-fraud or "1" for fraud).
        """
        try:
            # Establish a database connection
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                cursor = connection.cursor()

                # Prepare SQL query to insert a new record
                insert_query = f"""
                INSERT INTO {topic_name} (transaction_id, is_fraud)
                VALUES (%s, %s)
                """
                cursor.execute(insert_query, (transaction_id, prediction_result))
                connection.commit()

                print(f"Transaction {transaction_id} saved to mysql and its score is {prediction_result}\n")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
    
    def is_fraud(self) -> None:
        """
        Consumes messages from Kafka, predicts if the transaction is fraudulent, 
        and saves the result to the database.
        """
        
        # Initialize the Kafka consumer
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda trans: json.loads(trans.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit="false"
        )

        try:
            for id, transaction in enumerate(consumer):
                # Prepare input data for prediction
                x = np.array(list(map(float, transaction.value.values()))).reshape(1, -1)
                is_fraud = int(self.model.predict(x)[0])
                
                # Save the prediction result to the database
                self.save_to_database(id, is_fraud, self.topic_name)       
                                         
                # This is a simulation for a real-time streaming so just 100 transations are enough
                if id == 99:
                    break
        except Exception as e:
            print(f"Error consuming messages: {e}")
        finally:
            consumer.close()
            print("\n\nConsumer closed.")