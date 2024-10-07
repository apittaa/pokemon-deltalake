import json
import os
import sys
from datetime import datetime

import boto3
import requests
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from multiprocessing import Pool

from resources.s3_manager import create_s3_client


def get_pokemons_list(base_url: str, params: dict) -> dict:
    """
    Fetches Pokémon data from the given API endpoint, handling pagination and displaying progress.
    Args:
        base_url (str): The base URL of the Pokémon API endpoint.
        params (dict): A dictionary of query parameters to include in the API request.
    Returns:
        dict: A dictionary containing all Pokémon data fetched from the API, including an 'id' field.
    """

    # Make an initial request to get the total number of Pokemon
    response = requests.get(base_url, params=params)
    data = response.json()
    total_count = data["count"]  # total number of Pokemon available
    all_data = []

    # Use tqdm to display the progress
    with tqdm(total=total_count, desc="Fetching Pokémon data") as pbar:
        while True:
            # Add current page's results to the list
            for pokemon in data["results"]:
                # Extract the ID from the URL and add it to the pokemon data
                pokemon_id = int(pokemon["url"].split("/")[-2])
                pokemon["id"] = pokemon_id
                all_data.append(pokemon)

            # Update the progress bar
            pbar.update(len(data["results"]))

            # Check if there's a next page
            if data["next"]:
                # Get the next URL and fetch the next page
                next_url = data["next"]
                params["offset"] = int(next_url.split("offset=")[1].split("&")[0])
                response = requests.get(base_url, params=params)
                data = response.json()
            else:
                break

    return all_data


def fetch_pokemon_detail(args):
    """
    Helper function to fetch Pokémon details for a given ID and URL.
    Args:
        args (tuple): A tuple containing the key, base URL, and Pokémon ID.
    Returns:
        tuple: A tuple containing the key, Pokémon ID, and fetched data.
    """
    key, base_url, pokemon_id = args
    url = f"{base_url}{pokemon_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
        return key, pokemon_id, data
    except requests.RequestException as e:
        logger.error(f"Error fetching data for ID {pokemon_id} from {url}: {e}")
        return key, pokemon_id, None


def get_pokemons_details(urls: dict, pokemons_data: dict) -> dict:
    """
    Fetches Pokémon details from the given API endpoints using parallel processing.
    Args:
        urls (dict): A dictionary containing the base URLs for different types of Pokémon data.
        pokemons_data (dict): A dictionary containing the Pokémon data fetched from the API.
    Returns:
        dict: A dictionary containing all Pokémon details fetched from the API.
    """

    all_data = {"pokemon_details": [], "pokemon_species": []}

    tasks = []
    for pokemon in pokemons_data:
        pokemon_id = pokemon["id"]
        for key, base_url in urls.items():
            tasks.append((key, base_url, pokemon_id))

    with Pool(4) as p:
        for key, pokemon_id, data in tqdm(
            p.imap_unordered(fetch_pokemon_detail, tasks),
            total=len(tasks),
            desc="Fetching Pokémon details",
        ):
            if data:
                all_data[key].append(data)

    return all_data


def save_data(data: dict, s3_client: boto3.client, bucket: str) -> None:
    """
    Save the given data to an S3 bucket as separate JSON files.
    Args:
        data (dict): The data to be saved.
        s3_client (boto3.client): The Boto3 S3 client used to interact with S3.
        bucket (str): The name of the S3 bucket where the data will be saved.
    Returns:
        None
    Raises:
        Exception: If there is an error saving the data to the S3 bucket.
    """

    try:
        logger.info("Saving data to S3 bucket")
        now = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define the keys for the different types of data
        keys = {
            "pokemons_list": f"pokemons_raw/pokemons_list/{now}.json",
            "pokemon_details": f"pokemons_raw/pokemon_details/{now}.json",
            "pokemon_species": f"pokemons_raw/pokemon_species/{now}.json",
        }

        # Save each part of the data as a separate JSON file
        for key, s3_key in keys.items():
            json_bytes = json.dumps(data[key]).encode("utf-8")
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=json_bytes,
            )

        logger.success("Data saved to S3 bucket")
    except Exception as e:
        logger.error(f"Error saving data to S3 bucket: {e}")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Get the S3 credentials and bucket name
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    bucket = os.getenv("RAW_BUCKET")

    # Define the S3 endpoint URL
    s3_endpoint_url = "http://localhost:9000"

    # Define the API base URL and query parameters
    base_url = "https://pokeapi.co/api/v2/pokemon/"
    params = {"limit": 100, "offset": 0}

    # Define the URLs for the different types of data
    urls = {
        "pokemon_details": "https://pokeapi.co/api/v2/pokemon/",
        "pokemon_species": "https://pokeapi.co/api/v2/pokemon-species/",
    }

    # Create an S3 client
    s3_conn = create_s3_client(access_key, secret_key, s3_endpoint_url)

    # Get the data
    pokemons_data = get_pokemons_list(base_url, params)
    pokemons_details = get_pokemons_details(urls, pokemons_data)

    # Combine the data
    data = {"pokemons_list": pokemons_data, **pokemons_details}

    # Save the data to S3
    save_data(data, s3_conn, bucket)
