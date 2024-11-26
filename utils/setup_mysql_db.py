import mysql.connector
from mysql.connector import Error
from typing import Dict

"""
This module provides functionality to interact with a MySQL database, 
specifically to create a table for storing transaction data related to fraud detection.

The module includes a function, `create_table`, that creates a table in a MySQL database 
if it does not already exist. The table stores transaction details, including the 
transaction ID and whether the transaction is flagged as fraudulent.

Dependencies:
    - mysql.connector: To interact with the MySQL database.
"""

def create_table(table_name: str, db_config: Dict[str, str]) -> None:
    """
    Creates a table in the database if it doesn't already exist.

    Args:
        table_name (str): The name of the table to create.
                          This must be a valid table name according to MySQL naming conventions.
        db_config (dict): A dictionary containing the database configuration.
                          Example keys include:
                          - host (str): The database server host (e.g., 'localhost').
                          - user (str): The database user (e.g., 'root').
                          - password (str): The password for the database user.
                          - database (str): The name of the database to connect to.

    Returns:
        None

    Raises:
        mysql.connector.Error: If a connection or query error occurs.

    Example:
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'password123',
            'database': 'bank'
        }
        create_table('transactions', db_config)
    """
    connection = mysql.connector.connect(**db_config)
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                transaction_id INT PRIMARY KEY,  # Unique identifier for each transaction
                is_fraud BOOL                    # Indicates if the transaction is fraudulent
            );
            """
            cursor.execute(create_table_query)
            connection.commit()
            
            print(f"{table_name} table was successfully created in the database if it was not exist\n")    
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()