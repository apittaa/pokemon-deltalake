import duckdb
from loguru import logger
from typing import Any
import requests
from datetime import datetime
from dotenv import load_dotenv
import os
import boto3


def duckdb_manager():
    """
    Establishes a connection to an in-memory DuckDB database.

    This function attempts to connect to a DuckDB database stored in memory.
    If the connection is successful, it returns the connection object.
    If an error occurs during the connection attempt, it logs the error and returns None.

    Returns:
        duckdb.DuckDBPyConnection: The connection object to the DuckDB database if successful.
        None: If an error occurs during the connection attempt.
    """
    try:
        logger.info("Connecting to DuckDB")
        conn = duckdb.connect(database=":memory:")
        logger.info("Connected to DuckDB")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        return None

def query_executor(conn, query: str) -> None:   
    """
    Executes a given SQL query using the provided database connection.

    Args:
        conn: A database connection object that has an execute method.
        query (str): The SQL query to be executed.

    Logs:
        Logs an info message before executing the query.
        Logs a success message if the query is executed successfully.
        Logs an error message if there is an exception during query execution.

    Raises:
        Exception: If there is an error during query execution, it is caught and logged.
    """
    try:
        logger.info(f"Executing query")
        conn.execute(query)
        logger.success(f"Query executed successfully")
    except Exception as e:
        logger.error(f"Error executing query: {e}")

def s3_manager(access_key: str, secret_key: str, endpoint_url: str) -> Any:
    # Create and return your S3 client here using access_key and secret_key
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    return s3_client

def get_data(url):
    data = requests.get(url).json()
    
    data_save = data['results']

    return data_save

def save_data(data, s3_client):
    try:
        logger.info(f"Saving data to S3 bucket")
        now = datetime.now().strftime("%Y%m%d_%H%M%S")

        s3_client.put_object(
            Bucket="datalake",
            Key=f"pokemons_raw/pokemons_list/{now}.json",
            Body=str(data),
        )

        logger.success(f"Data saved to S3 bucket")
    except Exception as e:
        logger.error(f"Error saving data to S3 bucket: {e}")



if __name__ == "__main__":
    load_dotenv()
    
    url = "https://pokeapi.co/api/v2/pokemon?limit=2"
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    s3_endpoint_url = "http://s3service:9000"

    s3_conn = s3_manager(access_key, secret_key, s3_endpoint_url)
    data = get_data(url)
    save_data(data, s3_conn)