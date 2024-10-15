import json
import os
import sys
from datetime import datetime
from multiprocessing import Pool

import requests
from dagster import Definitions, asset
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from resources.s3_manager import create_s3_client

# Define the API base URL and query parameters
BASE_URL = "https://pokeapi.co/api/v2/pokemon/"


@asset
def get_pokemons_list() -> list:
    """
    Fetches a list of Pokémon data from the Pokémon API.
    This function retrieves Pokémon data in batches, iterating through
    the paginated results until all Pokémon data is fetched. Each Pokémon
    entry in the returned list includes an additional 'id' field extracted
    from the Pokémon's URL.
    Returns:
        list: A list of dictionaries, each containing data for a single Pokémon.
    """

    # Fetch the data from the API
    try:
        # Make an initial request to get the total number of Pokemon
        params = {"limit": 100, "offset": 0}
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        total_count = data["count"]
        all_data = []

        # Use tqdm to display the progress
        with tqdm(total=total_count, desc="Fetching Pokémon data") as pbar:
            while True:
                # Add current page's results to the list
                for pokemon in data["results"]:
                    pokemon_id = int(pokemon["url"].split("/")[-2])
                    pokemon["id"] = pokemon_id
                    all_data.append(pokemon)
                pbar.update(len(data["results"]))
                # Check if there's a next page
                if data["next"]:
                    # Get the next URL and fetch the next page
                    next_url = data["next"]
                    params["offset"] = int(next_url.split("offset=")[1].split("&")[0])
                    response = requests.get(BASE_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                else:
                    break

        return all_data
    # Handle any exceptions that occur during the request
    except requests.RequestException as e:
        logger.error(f"Error fetching Pokémon list: {e}")
        return []


def fetch_pokemon_detail(args: tuple) -> tuple:
    """
    Fetches the details of a Pokémon from a given URL.
    Args:
        args (tuple): A tuple containing:
            - key (str): A unique identifier for the Pokémon.
            - base_url (str): The base URL of the Pokémon API.
            - pokemon_id (int): The ID of the Pokémon to fetch.
    Returns:
        tuple: A tuple containing:
            - key (str): The unique identifier for the Pokémon.
            - pokemon_id (int): The ID of the Pokémon.
            - data (dict or None): The JSON response from the API if successful, otherwise None.
    Raises:
        requests.RequestException: If there is an error while making the request.
    """

    # Unpack the arguments
    key, base_url, pokemon_id = args
    url = f"{base_url}{pokemon_id}"

    # Fetch the data from the API
    try:
        # Make the request
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return key, pokemon_id, data
    # Handle any exceptions that occur during the request
    except requests.RequestException as e:
        logger.error(f"Error fetching data for ID {pokemon_id} from {url}: {e}")
        return key, pokemon_id, None


@asset
def get_pokemons_details(get_pokemons_list: list) -> dict:
    """
    Fetch detailed information for a list of Pokémon.
    This function retrieves detailed information for each Pokémon in the provided list
    by making asynchronous requests to the Pokémon API. It fetches both the Pokémon details
    and species information.
    Args:
        get_pokemons_list (list): A list of dictionaries, where each dictionary contains
                                at least the 'id' key representing the Pokémon ID.
    Returns:
        dict: A dictionary with keys 'pokemon_details' and 'pokemon_species', each containing
            a list of the respective details fetched from the API.
    """

    # Define the base URLs for fetching Pokémon details and species
    detail_urls = {
        "pokemon_details": "https://pokeapi.co/api/v2/pokemon/",
        "pokemon_species": "https://pokeapi.co/api/v2/pokemon-species/",
    }

    # Fetch the data from the API
    all_data = {key: [] for key in detail_urls.keys()}
    tasks = [
        (key, base_url, pokemon["id"])
        for pokemon in get_pokemons_list
        for key, base_url in detail_urls.items()
    ]

    # Use multiprocessing to fetch the data in parallel
    try:
        # Use tqdm to display the progress
        with Pool(4) as p:
            for key, _, data in tqdm(
                p.imap_unordered(fetch_pokemon_detail, tasks),
                total=len(tasks),
                desc="Fetching Pokémon details",
            ):
                if data:
                    all_data[key].append(data)
    # Handle any exceptions that occur during the request
    except Exception as e:
        logger.error(f"Error fetching Pokémon details: {e}")
        return {}

    return all_data


@asset
def process_raw_pokemon_data(
    get_pokemons_details: dict, get_pokemons_list: list
) -> None:
    """
    Save Pokémon data to an S3 bucket.
    This function saves Pokémon details and list data to an S3 bucket. It retrieves
    AWS credentials and bucket information from environment variables, combines the
    provided Pokémon details and list into a single dictionary, and uploads the data
    to the specified S3 bucket with timestamped keys.
    Args:
        get_pokemons_details (dict): A dictionary containing Pokémon details.
        get_pokemons_list (list): A list containing Pokémon data.
    Returns:
        None
    Raises:
        Exception: If there is an error during the S3 upload process, an exception is logged.
    """

    # Load the environment variables
    load_dotenv()
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    bucket = os.getenv("RAW_BUCKET")
    s3_endpoint_url = "http://localhost:9000"

    # Create an S3 client
    s3_client = create_s3_client(access_key, secret_key, s3_endpoint_url)

    # Save the data to the S3 bucket
    try:
        # Combine the data
        logger.info("Saving data to S3 bucket")
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        combined_data = {"pokemons_list": get_pokemons_list, **get_pokemons_details}
        keys = {
            "pokemons_list": f"pokemons_raw/pokemons_list/{now}.json",
            "pokemon_details": f"pokemons_raw/pokemon_details/{now}.json",
            "pokemon_species": f"pokemons_raw/pokemon_species/{now}.json",
        }
        # Upload the data to the S3 bucket
        for key, s3_key in keys.items():
            json_bytes = json.dumps(combined_data[key]).encode("utf-8")
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=json_bytes)

        logger.success("Data saved to S3 bucket")
    # Handle any exceptions that occur during the upload
    except Exception as e:
        logger.error(f"Error saving data to S3 bucket: {e}")


# Define the pipeline definitions
defs = Definitions(
    assets=[get_pokemons_list, get_pokemons_details, process_raw_pokemon_data]
)

if __name__ == "__main__":
    pokemons_list = get_pokemons_list()
    pokemons_details = get_pokemons_details(pokemons_list)
    process_raw_pokemon_data(pokemons_details, pokemons_list)
