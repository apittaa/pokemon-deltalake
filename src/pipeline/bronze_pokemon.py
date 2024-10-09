import os
import sys

import boto3
import duckdb
from deltalake import write_deltalake
from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from resources.duckdb_manager import create_duckdb_connection, execute_query
from resources.s3_manager import create_s3_client
from schemas.pokemon_species import BRONZE_POKEMON_SPECIES_SCHEMA
from schemas.pokemons_list import BRONZE_POKEMONS_LIST_SCHEMA


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

    logger.info(f"Getting the latest file from the S3 bucket folder: {folder}")
    response = s3_conn.list_objects_v2(Bucket=bucket, Prefix=folder)

    # Extract the filenames
    files = [content["Key"] for content in response.get("Contents", [])]

    # Sort the files by their name (assuming they are timestamped)
    files.sort(reverse=True)

    if files:
        # Return the latest file
        latest_file = os.path.basename(files[0])
        logger.success(f"Latest file found: {latest_file}")

        return latest_file
    else:
        logger.error(f"No files found in the specified folder: {folder}")
        return None


def get_and_save_data(
    conn: duckdb.DuckDBPyConnection,
    raw_bucket: str,
    bronze_bucket: str,
    raw_folder: str,
    bronze_folder: str,
    latest_raw_file: str,
    access_key: str,
    secret_key: str,
    aws_region: str,
    s3_endpoint_url: str,
    schema: dict,
) -> None:
    """
    Save data from a JSON file in an S3 bucket to a Delta Lake table in the same bucket.
    This function reads data from a JSON file stored in an S3 bucket using DuckDB,
    and then writes the data to a Delta Lake table in the same bucket.
    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
        raw_bucket (str): The name of the raw S3 bucket.
        bronze_bucket (str): The name of the bronze S3 bucket.
        raw_folder (str): The folder path within the raw S3 bucket.
        bronze_folder (str): The folder path within the bronze S3 bucket.
        latest_raw_file (str): The name of the latest JSON file to be read and saved as Delta Lake.
        schema (dict): The schema to be used for the Delta Lake table.
    Returns:
        None
    """

    # Remove the file extension
    latest_raw_file = os.path.splitext(latest_raw_file)[0]

    latest_raw_file_path = f"s3://{raw_bucket}/{raw_folder}/{latest_raw_file}.json"
    latest_bronze_file_path = f"s3://{bronze_bucket}/{bronze_folder}/{latest_raw_file}"

    # Query to read the latest file
    query = f"""
        SELECT
            *
        FROM read_json_auto('{latest_raw_file_path}')
    """
    try:
        logger.info(
            f"Saving data to S3 bucket in Delta Lake format for folder: {bronze_folder}"
        )
        # Execute the query and fetch the data
        data = execute_query(conn, query).fetchdf()

        # Set the S3 storage options
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_key,
            "AWS_SECRET_ACCESS_KEY": secret_key,
            "AWS_ENDPOINT_URL": s3_endpoint_url,
            "AWS_REGION": aws_region,
            "AWS_S3_USE_HTTPS": "0",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

        # Write the data to Delta Lake format
        write_deltalake(
            latest_bronze_file_path,
            data,
            schema=schema,
            storage_options=storage_options,
            mode="overwrite",
        )
        logger.success(
            f"Data saved to S3 bucket in Delta Lake format for folder: {bronze_folder}"
        )
    except Exception as e:
        logger.error(f"Error saving data to S3 bucket in Delta Lake format: {e}")


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

    # Folders where the raw data is stored
    raw_folders = [
        "pokemons_raw/pokemons_list",
        "pokemons_raw/pokemon_species",
        "pokemons_raw/pokemon_details",
    ]
    bronze_folders = [
        "pokemons_bronze/pokemons_list",
        "pokemons_bronze/pokemon_species",
        "pokemons_bronze/pokemon_details",
    ]

    # Schemas for each folder
    schemas = [
        BRONZE_POKEMONS_LIST_SCHEMA,
        BRONZE_POKEMON_SPECIES_SCHEMA,
        BRONZE_POKEMON_SPECIES_SCHEMA,
    ]

    # Create S3 client
    s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)

    # Create DuckDB connection
    duckdb_conn = create_duckdb_connection(access_key, secret_key, aws_region)

    for raw_folder, bronze_folder, schema in zip(raw_folders, bronze_folders, schemas):
        # Get the latest file from the S3 bucket
        latest_raw_file = get_latest_file_from_s3(raw_bucket, raw_folder, s3_conn)
        if latest_raw_file:
            # Save the data to the Bronze bucket
            get_and_save_data(
                duckdb_conn,
                raw_bucket,
                bronze_bucket,
                raw_folder,
                bronze_folder,
                latest_raw_file,
                access_key,
                secret_key,
                aws_region,
                s3_endpoint_url,
                schema,
            )
