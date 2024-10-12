from dagster import Definitions, load_assets_from_modules
from pokemon_datalake import assets  # type: ignore
from pokemon_datalake.assets import (  # type: ignore
    bronze_pokemon,
    gold_pokemon,
    raw_pokemon,
    silver_pokemon,
)

all_assets = load_assets_from_modules(
    [assets, raw_pokemon, bronze_pokemon, silver_pokemon, gold_pokemon]
)

defs = Definitions(
    assets=all_assets,
)
