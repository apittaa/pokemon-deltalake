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
    Save data from joined silver tables in an S3 bucket to a Delta Lake table in the same bucket.
    This function reads data from joined silver tables stored in an S3 bucket using DuckDB,
    and then writes the data to a Delta Lake table in the same bucket.
    Args:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
        silver_bucket (str): The name of the silver S3 bucket.
        gold_bucket (str): The name of the gold S3 bucket.
        silver_species_folder (str): The folder path within the silver S3 bucket for species data.
        silver_details_folder (str): The folder path within the silver S3 bucket for details data.
        latest_silver_species_file (str): The name of the latest species file to be read.
        latest_silver_details_file (str): The name of the latest details file to be read.
        schema (dict): The schema to be used for the Delta Lake table.
    Returns:
        None
    """

    latest_silver_species_file_path = (
        f"s3://{silver_bucket}/{silver_species_folder}/{latest_silver_species_file}"
    )
    latest_silver_details_file_path = (
        f"s3://{silver_bucket}/{silver_details_folder}/{latest_silver_details_file}"
    )
    latest_gold_file_path = (
        f"s3://{gold_bucket}/{gold_details_folder}/{latest_silver_details_file}"
    )

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

    try:
        logger.info(
            f"Saving joined data to S3 bucket in Delta Lake format for folder: {latest_gold_file_path}"
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
            latest_gold_file_path,
            data,
            schema=schema,
            storage_options=storage_options,
            mode="overwrite",
        )
        logger.success(
            f"Joined data saved to S3 bucket in Delta Lake format for folder: {gold_details_folder}"
        )
    except Exception as e:
        logger.error(f"Error saving joined data to S3 bucket in Delta Lake format: {e}")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Get the S3 credentials and bucket names
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

    # Create S3 client
    s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)

    # Create DuckDB connection
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
