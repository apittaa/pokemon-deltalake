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
from schemas.pokemon_details import GOLD_POKEMON_DETAILS_SCHEMA


def get_latest_folder_from_s3(bucket: str, prefix: str, s3_conn: boto3.client) -> str:
    """
    Get the latest folder from an S3 bucket prefix.

    This function retrieves the latest folder from a specified prefix in an S3 bucket.
    It lists all the folders in the prefix, sorts them by their names in descending order,
    and returns the name of the latest folder.

    Args:
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix path within the S3 bucket.
        s3_conn (boto3.client): The S3 client connection object.

    Returns:
        str: The name of the latest folder in the specified prefix.
    """

    # Extract the latest folder from the S3 bucket prefix
    try:
        # List objects in the specified prefix
        logger.info(f"Getting the latest folder from the S3 bucket prefix: {prefix}")
        response = s3_conn.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")

        # Extract the folder names
        folders = [content["Prefix"] for content in response.get("CommonPrefixes", [])]

        # Sort the folders by their name (assuming they are timestamped)
        folders.sort(reverse=True)

        if folders:
            # Get the latest folder path
            latest_folder_path = folders[0]

            # List objects within the latest folder to find the latest subfolder
            response = s3_conn.list_objects_v2(
                Bucket=bucket, Prefix=latest_folder_path, Delimiter="/"
            )
            subfolders = [
                content["Prefix"] for content in response.get("CommonPrefixes", [])
            ]

            # Sort the subfolders by their name (assuming they are timestamped)
            subfolders.sort(reverse=True)

            if subfolders:
                # Return the latest subfolder
                latest_subfolder = os.path.basename(os.path.normpath(subfolders[0]))
                logger.success(f"Latest subfolder found: {latest_subfolder}")

                return latest_subfolder
            else:
                logger.error(
                    f"No subfolders found in the latest folder: {latest_folder_path}"
                )
                return None
        else:
            logger.error(f"No folders found in the specified prefix: {prefix}")
            return None
    # Handle exceptions
    except Exception as e:
        logger.error(f"Error getting the latest folder from S3: {e}")
        return None


def get_and_save_data(
    conn: duckdb.DuckDBPyConnection,
    silver_bucket: str,
    gold_bucket: str,
    silver_species_folder: str,
    silver_details_folder: str,
    gold_details_folder: str,
    latest_silver_species_file: str,
    latest_silver_details_file: str,
    access_key: str,
    secret_key: str,
    aws_region: str,
    s3_endpoint_url: str,
    schema: dict,
) -> None:
    """
    Fetches data from the silver layer, joins it, and saves the result to the gold layer in Delta Lake format.
    Args:
        conn (duckdb.DuckDBPyConnection): Connection to the DuckDB database.
        silver_bucket (str): Name of the S3 bucket containing the silver data.
        gold_bucket (str): Name of the S3 bucket where the gold data will be saved.
        silver_species_folder (str): Folder in the silver bucket containing species data.
        silver_details_folder (str): Folder in the silver bucket containing details data.
        gold_details_folder (str): Folder in the gold bucket where the joined data will be saved.
        latest_silver_species_file (str): Filename of the latest species data file in the silver bucket.
        latest_silver_details_file (str): Filename of the latest details data file in the silver bucket.
        access_key (str): AWS access key for S3 access.
        secret_key (str): AWS secret key for S3 access.
        aws_region (str): AWS region where the S3 buckets are located.
        s3_endpoint_url (str): Endpoint URL for S3.
        schema (dict): Schema definition for the Delta Lake table.
    Returns:
        None
    """

    # Set the file paths
    latest_silver_species_file_path = (
        f"s3://{silver_bucket}/{silver_species_folder}/{latest_silver_species_file}"
    )
    latest_silver_details_file_path = (
        f"s3://{silver_bucket}/{silver_details_folder}/{latest_silver_details_file}"
    )
    latest_gold_file_path = (
        f"s3://{gold_bucket}/{gold_details_folder}/{latest_silver_details_file}"
    )

    # Define the query to join the species and details data
    query = f"""
        SELECT
            pd.id,
            pd.name,
            pd.base_experience,
            pd.height,
            pd.weight,
            pd.abilities,
            pd.hp_stat,
            pd.attack_stat,
            pd.defense_stat,
            pd.special_attack_stat,
            pd.special_defense_stat,
            pd.speed_stat,
            pd.types,
            ps.capture_rate,
            ps.base_happiness,
            ps.is_baby,
            ps.is_legendary,
            ps.is_mythical,
            ps.growth_rate,
            ps.egg_groups,
            ps.color,
            ps.shape,
            ps.evolves_from_species,
            ps.habitat,
            ps.generation,
            ps.varieties
        FROM delta_scan('{latest_silver_details_file_path}') pd
        LEFT JOIN delta_scan('{latest_silver_species_file_path}') ps
        ON pd.id = ps.id
        ORDER BY pd.id;
    """

    # Save the joined data to Delta Lake format
    try:
        # Execute the query and fetch the data
        logger.info(
            f"Saving joined data to {latest_gold_file_path} in Delta Lake format"
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

        # Save the data to Delta Lake format
        write_deltalake(
            latest_gold_file_path,
            data,
            schema=schema,
            storage_options=storage_options,
            mode="overwrite",
        )
        logger.success(
            f"Data successfully saved to {gold_details_folder} in Delta Lake format"
        )
    # Handle exceptions
    except Exception as e:
        logger.error(f"Error saving data to Delta Lake: {e}")


@asset(deps=["process_silver_pokemon_data"])
def process_gold_pokemon_data() -> None:
    """
    Processes and transforms Pokémon data from the silver data lake to the gold data lake.
    This function performs the following steps:
    1. Loads environment variables for AWS credentials and bucket names.
    2. Establishes connections to S3 and DuckDB.
    3. Retrieves the latest files from the silver data lake.
    4. Joins the data from the latest files.
    5. Saves the transformed data to the gold data lake.
    Environment Variables:
    - AWS_REGION: The AWS region where the S3 buckets are located.
    - ACCESS_KEY: The AWS access key for authentication.
    - SECRET_KEY: The AWS secret key for authentication.
    - SILVER_BUCKET: The name of the S3 bucket containing the silver data.
    - GOLD_BUCKET: The name of the S3 bucket where the gold data will be stored.
    Raises:
    - Any exceptions raised during the process will propagate up and should be handled by the caller.
    """

    try:
        # Load environment variables
        load_dotenv()

        aws_region = os.getenv("AWS_REGION")
        access_key = os.getenv("ACCESS_KEY")
        secret_key = os.getenv("SECRET_KEY")
        silver_bucket = os.getenv("SILVER_BUCKET")
        gold_bucket = os.getenv("GOLD_BUCKET")

        # Define the S3 endpoint URL
        s3_endpoint_url = "http://localhost:9000"

        # Folders where the raw data is stored
        silver_species_folder = "pokemons_silver/pokemon_species"
        silver_details_folder = "pokemons_silver/pokemon_details"
        gold_details_folder = "pokemons_gold/pokemon_details"

        # Schemas for each folder
        schema = GOLD_POKEMON_DETAILS_SCHEMA

        # Create S3 client and DuckDB connection
        s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)
        duckdb_conn = create_duckdb_connection(access_key, secret_key, aws_region)

        # Get the latest files from the S3 bucket
        latest_silver_species_file = get_latest_folder_from_s3(
            silver_bucket, silver_species_folder, s3_conn
        )
        latest_silver_details_file = get_latest_folder_from_s3(
            silver_bucket, silver_details_folder, s3_conn
        )

        if latest_silver_species_file and latest_silver_details_file:
            # Save the joined data to the gold bucket
            get_and_save_data(
                duckdb_conn,
                silver_bucket,
                gold_bucket,
                silver_species_folder,
                silver_details_folder,
                gold_details_folder,
                latest_silver_species_file,
                latest_silver_details_file,
                access_key,
                secret_key,
                aws_region,
                s3_endpoint_url,
                schema,
            )
    except Exception as e:
        logger.error(f"Error processing gold Pokémon data: {e}")


defs = Definitions(assets=[process_gold_pokemon_data])

if __name__ == "__main__":
    process_gold_pokemon_data()
