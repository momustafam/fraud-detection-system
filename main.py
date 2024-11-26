import os
import warnings
import threading
from time import sleep
# from dotenv import load_dotenv
from utils.setup_mysql_db import create_table
from fraud_detection_system.transaction_producer import TransactionProducer
from fraud_detection_system.fraud_detection_system import FraudDetectionSystem

# Ignore specific warning
warnings.filterwarnings("ignore")


def main() -> None:
    """
    The main function that simulates a real-time fraud detection system.
    """

    # Load environment variables
    # load_dotenv()

    
    kafka_servers = os.getenv("KAFKA_BROKER").split(",")
    topic_name = os.getenv("TOPIC_NAME")
    data_path = os.getenv("DATA_PATH")
    model_path = os.getenv("MODEL_PATH")

    db_config = {
        'host': os.getenv("MYSQL_HOST"),
        'user': os.getenv("MYSQL_USER"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'database': os.getenv("MYSQL_DATABASE")
    }

    # Create transaction table in the database to save the model predictions
    try:
        create_table(table_name=topic_name, db_config=db_config)
    except Exception as e:
        print(f"Error creating database table: {e}\n")

    # Initialize the TransactionProducer to produce transaction data
    producer = TransactionProducer(
        data_path=data_path, 
        kafka_servers=kafka_servers, 
        topic_name=topic_name
    )

    # Initialize the FraudDetectionSystem to detect fraudulent transactions
    fd = FraudDetectionSystem(
        db_config=db_config,
        model_path=model_path,
        kafka_servers=kafka_servers,
        topic_name=topic_name
    )
    
    # Create threads to run both producer and fraud detection in parallel
    producer_thread = threading.Thread(target=producer.produce_transaction)
    fraud_detection_thread = threading.Thread(target=fd.is_fraud)

    # Start both threads
    producer_thread.start()
    fraud_detection_thread.start()

    # Wait for both threads to finish
    producer_thread.join()
    fraud_detection_thread.join()
    
if __name__ == "__main__":
    main()