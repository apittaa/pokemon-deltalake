import duckdb
from loguru import logger
from dotenv import load_dotenv
import os
import boto3

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from resources.duckdb_manager import create_duckdb_connection, execute_query
from resources.s3_manager import create_s3_client


def get_latest_file_from_s3(bucket: str, folder: str, s3_conn: boto3.client) -> str:
    """
    Get the latest file from an S3 bucket folder.

    This function retrieves the latest file from a specified folder in an S3 bucket.
    It lists all the files in the folder, sorts them by their names in descending order,
    and returns the name of the latest file.

    Args:
        bucket (str): The name of the S3 bucket.
        folder (str): The folder path within the S3 bucket.
        s3_conn (boto3.client): The S3 client connection object.

    Returns:
        str: The name of the latest file in the specified folder.
    """

    logger.info("Getting the latest file from the S3 bucket")
    response = s3_conn.list_objects_v2(Bucket=bucket, Prefix=folder)

    # Extract the filenames
    files = [content["Key"] for content in response.get("Contents", [])]

    # Sort the files by their name (assuming they are timestamped)
    files.sort(reverse=True)

    if files:
        # Return the latest file
        latest_file = os.path.basename(files[0])
        logger.success(f"Latest file found: { latest_file}")

        return latest_file
    else:
        logger.error("No files found in the specified folder.")


def get_and_save_data(
    conn: duckdb.DuckDBPyConnection,
    raw_bucket: str,
    bronze_bucket: str,
    raw_folder: str,
    bronze_folder: str,
    latest_raw_file: str,
) -> None:
    """
    Save data from a JSON file in an S3 bucket to a Parquet file in the same bucket.
    This function reads data from a JSON file stored in an S3 bucket using DuckDB,
    and then writes the data to a Parquet file in the same bucket.
    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
        bucket (str): The name of the S3 bucket.
        latest_file (str): The name of the latest JSON file to be read and saved as Parquet.
    Returns:
        None
    """

    # Remove the file extension
    latest_raw_file = os.path.splitext(latest_raw_file)[0]

    logger.info("Saving data to S3 bucket in Parquet format")
    latest_raw_file_path = f"s3://{raw_bucket}/{raw_folder}/{latest_raw_file}"

    latest_bronze_file_path = f"s3://{bronze_bucket}/{bronze_folder}/{latest_raw_file}"

    # Query to read the latest file
    query = f"""
        COPY (
            SELECT
                *
            FROM read_json_auto('{latest_raw_file_path}.json')
        )
        TO '{latest_bronze_file_path}.parquet'
        (FORMAT PARQUET)
    """
    try:
        execute_query(conn, query)
        logger.success("Data saved to S3 bucket in Parquet format")
    except Exception as e:
        logger.error(f"Error executing query: {e}")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Get the S3 credentials and bucket names
    aws_region = os.getenv("AWS_REGION")
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    raw_bucket = os.getenv("RAW_BUCKET")
    bronze_bucket = os.getenv("BRONZE_BUCKET")

    # Define the S3 endpoint URL
    s3_endpoint_url = "http://localhost:9000"

    # Folder where the raw data is stored
    raw_folder = "pokemons_raw/pokemons_list"
    bronze_folder = "pokemons_bronze/pokemons_list"

    # Create S3 client
    s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)

    # Get the latest file from the S3 bucket
    latest_raw_file = get_latest_file_from_s3(raw_bucket, raw_folder, s3_conn)

    # Create DuckDB connection
    duckdb_conn = create_duckdb_connection(access_key, secret_key, aws_region)

    # Save the data to the Bronze bucket
    get_and_save_data(
        duckdb_conn,
        raw_bucket,
        bronze_bucket,
        raw_folder,
        bronze_folder,
        latest_raw_file,
    )
