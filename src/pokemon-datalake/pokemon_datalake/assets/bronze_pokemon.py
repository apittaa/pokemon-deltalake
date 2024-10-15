import os
import sys

import boto3
import duckdb
from dagster import Definitions, asset
from deltalake import write_deltalake
from dotenv import load_dotenv
from loguru import logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from resources.duckdb_manager import create_duckdb_connection, execute_query
from resources.s3_manager import create_s3_client
from schemas.pokemon_details import BRONZE_POKEMON_DETAILS_SCHEMA
from schemas.pokemon_species import BRONZE_POKEMON_SPECIES_SCHEMA
from schemas.pokemons_list import BRONZE_POKEMONS_LIST_SCHEMA

# from pokemon_datalake.assets.raw_pokemon import save_data_to_s3


def get_latest_file_from_s3(bucket: str, folder: str, s3_conn: boto3.client) -> str:
    """
    Fetches the latest file from a specified S3 bucket folder.
    This function lists all objects in the specified S3 bucket folder, sorts them
    by their keys in descending order (assuming the keys are timestamped), and
    returns the name of the latest file.
    Args:
        bucket (str): The name of the S3 bucket.
        folder (str): The folder path within the S3 bucket.
        s3_conn (boto3.client): The Boto3 S3 client instance.
    Returns:
        str: The name of the latest file in the specified folder, or None if no files are found.
    """

    # Extract the latest file from the S3 bucket folder
    try:
        # List objects in the specified folder
        logger.info(f"Fetching the latest file from S3 bucket folder: {folder}")
        response = s3_conn.list_objects_v2(Bucket=bucket, Prefix=folder)

        # Extract and sort the filenames by timestamp (assuming files are timestamped)
        files = sorted(
            [content["Key"] for content in response.get("Contents", [])], reverse=True
        )
        # Return the latest file
        if files:
            # Extract the latest file name
            latest_file = os.path.basename(files[0])
            logger.success(f"Found latest file: {latest_file}")
            return latest_file
        else:
            logger.error(f"No files found in the specified folder: {folder}")
            return None
    # Handle exceptions
    except Exception as e:
        logger.error(f"Error fetching the latest file from S3: {e}")
        return None


def save_data_to_delta_lake(
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
    Save data from a raw JSON file in an S3 bucket to a Delta Lake format in another S3 bucket.
    Args:
        conn (duckdb.DuckDBPyConnection): Connection to DuckDB.
        raw_bucket (str): Name of the S3 bucket containing the raw data.
        bronze_bucket (str): Name of the S3 bucket where the data will be saved in Delta Lake format.
        raw_folder (str): Folder path in the raw S3 bucket.
        bronze_folder (str): Folder path in the bronze S3 bucket.
        latest_raw_file (str): Name of the latest raw file to be processed.
        access_key (str): AWS access key.
        secret_key (str): AWS secret key.
        aws_region (str): AWS region.
        s3_endpoint_url (str): S3 endpoint URL.
        schema (dict): Schema definition for the Delta Lake table.
    Returns:
        None
    Raises:
        Exception: If there is an error during the data saving process.
    """

    # Remove the file extension
    latest_raw_file = os.path.splitext(latest_raw_file)[0]

    # Define the file paths
    raw_file_path = f"s3://{raw_bucket}/{raw_folder}/{latest_raw_file}.json"
    bronze_file_path = f"s3://{bronze_bucket}/{bronze_folder}/{latest_raw_file}"

    # Define the SQL query to read the JSON file
    query = f"SELECT * FROM read_json_auto('{raw_file_path}')"

    # Save the data to Delta Lake format
    try:
        # Execute the query and fetch the data
        logger.info(
            f"Saving data from {raw_file_path} to {bronze_file_path} in Delta Lake format"
        )
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

        write_deltalake(
            bronze_file_path,
            data,
            schema=schema,
            storage_options=storage_options,
            mode="overwrite",
        )
        logger.success(
            f"Data successfully saved to {bronze_file_path} in Delta Lake format"
        )
    # Handle any exceptions that occur during the data saving process
    except Exception as e:
        logger.error(f"Error saving data to Delta Lake: {e}")


@asset(deps=["process_raw_pokemon_data"])
def process_bronze_pokemon_data() -> None:
    """
    Processes raw Pokémon data and saves it to a bronze data lake.
    This function performs the following steps:
    1. Loads environment variables for AWS credentials and bucket names.
    2. Defines the raw and bronze folder paths and their corresponding schemas.
    3. Creates connections to S3 and DuckDB.
    4. Iterates over the raw folders, retrieves the latest raw file from S3,
       and saves the processed data to the bronze data lake using the specified schema.
    Environment Variables:
    - AWS_REGION: The AWS region.
    - ACCESS_KEY: The AWS access key.
    - SECRET_KEY: The AWS secret key.
    - RAW_BUCKET: The S3 bucket name for raw data.
    - BRONZE_BUCKET: The S3 bucket name for bronze data.
    Raises:
    - Any exceptions raised by the underlying functions such as S3 connection,
      DuckDB connection, or data processing functions.
    Returns:
    - None
    """

    try:
        # Load environment variables
        load_dotenv()

        aws_region = os.getenv("AWS_REGION")
        access_key = os.getenv("ACCESS_KEY")
        secret_key = os.getenv("SECRET_KEY")
        raw_bucket = os.getenv("RAW_BUCKET")
        bronze_bucket = os.getenv("BRONZE_BUCKET")

        # Define the S3 endpoint URL
        s3_endpoint_url = "http://localhost:9000"

        # Define the raw and bronze folders and schemas
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
        schemas = [
            BRONZE_POKEMONS_LIST_SCHEMA,
            BRONZE_POKEMON_SPECIES_SCHEMA,
            BRONZE_POKEMON_DETAILS_SCHEMA,
        ]

        # Create S3 client and DuckDB connection
        s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)
        duckdb_conn = create_duckdb_connection(access_key, secret_key, aws_region)

        # Process the raw data and save it to the bronze data lake
        for raw_folder, bronze_folder, schema in zip(
            raw_folders, bronze_folders, schemas
        ):
            latest_raw_file = get_latest_file_from_s3(raw_bucket, raw_folder, s3_conn)
            if latest_raw_file:
                save_data_to_delta_lake(
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
    #
    except Exception as e:
        logger.error(f"Error processing bronze Pokémon data: {e}")


defs = Definitions(assets=[process_bronze_pokemon_data])

if __name__ == "__main__":
    process_bronze_pokemon_data()
