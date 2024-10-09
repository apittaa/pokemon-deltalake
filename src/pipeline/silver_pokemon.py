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
from schemas.pokemon_details import SILVER_POKEMON_DETAILS_SCHEMA
from schemas.pokemon_species import SILVER_POKEMON_SPECIES_SCHEMA


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


def get_and_save_data(
    conn: duckdb.DuckDBPyConnection,
    bronze_bucket: str,
    silver_bucket: str,
    bronze_folder: str,
    silver_folder: str,
    latest_bronze_file: str,
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
        bronze_bucket (str): The name of the bronze S3 bucket.
        silver_bucket (str): The name of the silver S3 bucket.
        bronze_folder (str): The folder path within the bronze S3 bucket.
        silver_folder (str): The folder path within the silver S3 bucket.
        latest_bronze_file (str): The name of the latest JSON file to be read and saved as Delta Lake.
        schema (dict): The schema to be used for the Delta Lake table.
    Returns:
        None
    """

    latest_bronze_file_path = (
        f"s3://{bronze_bucket}/{bronze_folder}/{latest_bronze_file}"
    )
    latest_silver_file_path = (
        f"s3://{silver_bucket}/{silver_folder}/{latest_bronze_file}"
    )

    # Determine the query based on the folder
    if "pokemon_species" in bronze_folder:
        query = f"""
            SELECT
                id
                , name
                , capture_rate
                , base_happiness
                , is_baby
                , is_legendary
                , is_mythical
                , growth_rate.name AS growth_rate
                , array_to_string(json_extract_string(egg_groups, '$[*].name'), ', ') AS egg_groups
                , color.name AS color
                , shape.name AS shape
                , evolves_from_species.name AS evolves_from_species
                , habitat.name AS habitat
                , generation.name AS generation
                , array_to_string(json_extract_string(varieties, '$[*].pokemon.name'), ', ') AS varieties
            FROM delta_scan('{latest_bronze_file_path}')
            ORDER BY id;
        """
    elif "pokemon_details" in bronze_folder:
        query = f"""
            SELECT
                id
                , name
                , base_experience
                , height
                , weight
                , array_to_string(json_extract_string(abilities, '$[*].ability.name'), ', ') AS abilities
                , json_extract_string(stats, '$[0].base_stat') AS hp_stat
                , json_extract_string(stats, '$[1].base_stat') AS attack_stat
                , json_extract_string(stats, '$[2].base_stat') AS defense_stat
                , json_extract_string(stats, '$[3].base_stat') AS special_attack_stat
                , json_extract_string(stats, '$[4].base_stat') AS special_defense_stat
                , json_extract_string(stats, '$[5].base_stat') AS speed_stat
                , array_to_string(json_extract_string(types, '$[*].type.name'), ', ') AS types
            FROM delta_scan('{latest_bronze_file_path}')
            ORDER BY id;
        """
    else:
        logger.error(f"Unknown folder type: {bronze_folder}")
        return

    try:
        logger.info(
            f"Saving data to S3 bucket in Delta Lake format for folder: {silver_folder}"
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
            latest_silver_file_path,
            data,
            schema=schema,
            storage_options=storage_options,
            mode="overwrite",
        )
        logger.success(
            f"Data saved to S3 bucket in Delta Lake format for folder: {silver_folder}"
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
    bronze_bucket = os.getenv("BRONZE_BUCKET")
    silver_bucket = os.getenv("SILVER_BUCKET")

    # Define the S3 endpoint URL
    s3_endpoint_url = "http://localhost:9000"

    # Folders where the raw data is stored
    bronze_folders = [
        "pokemons_bronze/pokemon_species",
        "pokemons_bronze/pokemon_details",
    ]
    silver_folders = [
        "pokemons_silver/pokemon_species",
        "pokemons_silver/pokemon_details",
    ]

    # Schemas for each folder
    schemas = [SILVER_POKEMON_SPECIES_SCHEMA, SILVER_POKEMON_DETAILS_SCHEMA]

    # Create S3 client
    s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)

    # Create DuckDB connection
    duckdb_conn = create_duckdb_connection(access_key, secret_key, aws_region)

    for bronze_folder, silver_folder, schema in zip(
        bronze_folders, silver_folders, schemas
    ):
        # Get the latest file from the S3 bucket
        latest_bronze_file = get_latest_folder_from_s3(
            bronze_bucket, bronze_folder, s3_conn
        )
        if latest_bronze_file:
            # Save the data to the silver bucket
            get_and_save_data(
                duckdb_conn,
                bronze_bucket,
                silver_bucket,
                bronze_folder,
                silver_folder,
                latest_bronze_file,
                access_key,
                secret_key,
                aws_region,
                s3_endpoint_url,
                schema,
            )
