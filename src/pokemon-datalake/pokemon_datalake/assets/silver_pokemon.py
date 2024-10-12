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
from schemas.pokemon_details import SILVER_POKEMON_DETAILS_SCHEMA
from schemas.pokemon_species import SILVER_POKEMON_SPECIES_SCHEMA

# from pokemon_datalake.assets.bronze_pokemon import process_bronze_pokemon_data


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

    try:
        # Get the list of folders in the specified prefix
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
    except Exception as e:
        logger.error(f"Error retrieving the latest folder from S3: {e}")
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
    Parameters:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
        bronze_bucket (str): The name of the S3 bucket containing the bronze data.
        silver_bucket (str): The name of the S3 bucket where the silver data will be saved.
        bronze_folder (str): The folder path within the bronze bucket.
        silver_folder (str): The folder path within the silver bucket.
        latest_bronze_file (str): The name of the latest bronze file to be processed.
        access_key (str): AWS access key for S3 authentication.
        secret_key (str): AWS secret key for S3 authentication.
        aws_region (str): AWS region where the S3 buckets are located.
        s3_endpoint_url (str): The endpoint URL for the S3 service.
        schema (dict): The schema definition for the Delta Lake table.

    Returns:
        None

    Raises:
        Exception: If there is an error saving data to Delta Lake.

    Logs:
        Logs information about the saving process and any errors encountered.
    """

    # Define the file paths for the bronze and silver data
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
                id,
                name,
                capture_rate,
                base_happiness,
                is_baby,
                is_legendary,
                is_mythical,
                growth_rate.name AS growth_rate,
                array_to_string(json_extract_string(egg_groups, '$[*].name'), ', ') AS egg_groups,
                color.name AS color,
                shape.name AS shape,
                evolves_from_species.name AS evolves_from_species,
                habitat.name AS habitat,
                generation.name AS generation,
                array_to_string(json_extract_string(varieties, '$[*].pokemon.name'), ', ') AS varieties
            FROM delta_scan('{latest_bronze_file_path}')
            ORDER BY id;
        """
    elif "pokemon_details" in bronze_folder:
        query = f"""
            SELECT
                id,
                name,
                base_experience,
                height,
                weight,
                array_to_string(json_extract_string(abilities, '$[*].ability.name'), ', ') AS abilities,
                json_extract_string(stats, '$[0].base_stat') AS hp_stat,
                json_extract_string(stats, '$[1].base_stat') AS attack_stat,
                json_extract_string(stats, '$[2].base_stat') AS defense_stat,
                json_extract_string(stats, '$[3].base_stat') AS special_attack_stat,
                json_extract_string(stats, '$[4].base_stat') AS special_defense_stat,
                json_extract_string(stats, '$[5].base_stat') AS speed_stat,
                array_to_string(json_extract_string(types, '$[*].type.name'), ', ') AS types
            FROM delta_scan('{latest_bronze_file_path}')
            ORDER BY id;
        """
    else:
        logger.error(f"Unknown folder type: {bronze_folder}")
        return

    # Save the data to Delta Lake format
    try:
        # Fetch the data from DuckDB
        logger.info(f"Saving data to {latest_silver_file_path} in Delta Lake format")
        data = execute_query(conn, query).fetchdf()

        # Define the storage options for the Delta Lake table
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
            latest_silver_file_path,
            data,
            schema=schema,
            storage_options=storage_options,
            mode="overwrite",
        )
        logger.success(
            f"Data successfully saved to {silver_folder} in Delta Lake format"
        )
    # Handle any exceptions that occur during the data saving process
    except Exception as e:
        logger.error(f"Error saving data to Delta Lake: {e}")


@asset(deps=["process_bronze_pokemon_data"])
def process_silver_pokemon_data() -> None:
    """
    Processes silver-tier Pokémon data by performing the following steps:
    1. Loads environment variables using `load_dotenv()`.
    2. Retrieves AWS credentials and bucket names from environment variables.
    3. Defines the folders for bronze and silver data and their corresponding schemas.
    4. Creates connections to S3 and DuckDB using the provided credentials.
    5. Iterates over the bronze and silver folders, processing the latest bronze data file
       and saving the processed data to the corresponding silver folder in S3.
    Environment Variables:
    - AWS_REGION: The AWS region for the S3 bucket.
    - ACCESS_KEY: The access key for AWS authentication.
    - SECRET_KEY: The secret key for AWS authentication.
    - BRONZE_BUCKET: The name of the S3 bucket containing bronze data.
    - SILVER_BUCKET: The name of the S3 bucket to save silver data.
    Raises:
    - Any exceptions raised by the underlying functions such as `create_s3_client`,
      `create_duckdb_connection`, `get_latest_folder_from_s3`, and `get_and_save_data`.
    """

    try:
        # Load environment variables
        load_dotenv()

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

        # Process the silver-tier data for each folder
        for bronze_folder, silver_folder, schema in zip(
            bronze_folders, silver_folders, schemas
        ):
            latest_bronze_file = get_latest_folder_from_s3(
                bronze_bucket, bronze_folder, s3_conn
            )
            if latest_bronze_file:
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
    except Exception as e:
        logger.error(f"Error processing silver-tier Pokémon data: {e}")


defs = Definitions(assets=[process_silver_pokemon_data])

if __name__ == "__main__":
    process_silver_pokemon_data()
