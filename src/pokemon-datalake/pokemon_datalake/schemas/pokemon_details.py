import pyarrow as pa

# Define the schema using PyArrow fields
BRONZE_POKEMON_DETAILS_SCHEMA = pa.schema(
    [
        (
            "abilities",
            pa.list_(
                pa.struct(
                    [
                        (
                            "ability",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                        ("is_hidden", pa.bool_()),
                        ("slot", pa.int64()),
                    ]
                )
            ),
        ),
        ("base_experience", pa.int64()),
        ("cries", pa.struct([("latest", pa.string()), ("legacy", pa.string())])),
        ("forms", pa.list_(pa.struct([("name", pa.string()), ("url", pa.string())]))),
        (
            "game_indices",
            pa.list_(
                pa.struct(
                    [
                        ("game_index", pa.int64()),
                        (
                            "version",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        ("height", pa.int64()),
        (
            "held_items",
            pa.list_(
                pa.struct(
                    [
                        (
                            "item",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                        (
                            "version_details",
                            pa.list_(
                                pa.struct(
                                    [  # Fix for version_details
                                        ("rarity", pa.int64()),
                                        (
                                            "version",
                                            pa.struct(
                                                [
                                                    ("name", pa.string()),
                                                    ("url", pa.string()),
                                                ]
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        ("id", pa.int64()),
        ("is_default", pa.bool_()),
        ("location_area_encounters", pa.string()),
        (
            "moves",
            pa.list_(
                pa.struct(
                    [  # Fix for moves
                        (
                            "move",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                        (
                            "version_group_details",
                            pa.list_(
                                pa.struct(
                                    [  # Fix for version_group_details
                                        ("level_learned_at", pa.int64()),
                                        (
                                            "move_learn_method",
                                            pa.struct(
                                                [
                                                    ("name", pa.string()),
                                                    ("url", pa.string()),
                                                ]
                                            ),
                                        ),
                                        (
                                            "version_group",
                                            pa.struct(
                                                [
                                                    ("name", pa.string()),
                                                    ("url", pa.string()),
                                                ]
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        ("name", pa.string()),
        ("order", pa.int64()),
        (
            "past_abilities",
            pa.list_(
                pa.struct(
                    [
                        (
                            "abilities",
                            pa.list_(
                                pa.struct(
                                    [
                                        (
                                            "ability",
                                            pa.struct(
                                                [
                                                    ("name", pa.string()),
                                                    ("url", pa.string()),
                                                ]
                                            ),
                                        ),
                                        ("is_hidden", pa.bool_()),
                                        ("slot", pa.int64()),
                                    ]
                                )
                            ),
                        ),
                        (
                            "version",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        (
            "past_types",
            pa.list_(
                pa.struct(
                    [
                        (
                            "generation",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                        (
                            "types",
                            pa.list_(
                                pa.struct(
                                    [
                                        ("slot", pa.int64()),
                                        (
                                            "type",
                                            pa.struct(
                                                [
                                                    ("name", pa.string()),
                                                    ("url", pa.string()),
                                                ]
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
        ("species", pa.struct([("name", pa.string()), ("url", pa.string())])),
        (
            "sprites",
            pa.struct(
                [
                    ("back_default", pa.string()),
                    ("back_female", pa.string()),
                    ("back_shiny", pa.string()),
                    ("back_shiny_female", pa.string()),
                    ("front_default", pa.string()),
                    ("front_female", pa.string()),
                    ("front_shiny", pa.string()),
                    ("front_shiny_female", pa.string()),
                ]
            ),
        ),
        (
            "stats",
            pa.list_(
                pa.struct(
                    [
                        ("base_stat", pa.int64()),
                        ("effort", pa.int64()),
                        (
                            "stat",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        (
            "types",
            pa.list_(
                pa.struct(
                    [
                        ("slot", pa.int64()),
                        (
                            "type",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        ("weight", pa.int64()),
    ]
)

# Define the bronze schema using PyArrow
SILVER_POKEMON_DETAILS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),  # BIGINT
        pa.field("name", pa.string()),  # VARCHAR
        pa.field("base_experience", pa.int64()),  # BIGINT
        pa.field("height", pa.int64()),  # BIGINT
        pa.field("weight", pa.int64()),  # BIGINT
        pa.field("abilities", pa.string()),  # VARCHAR
        pa.field("hp_stat", pa.string()),  # VARCHAR
        pa.field("attack_stat", pa.string()),  # VARCHAR
        pa.field("defense_stat", pa.string()),  # VARCHAR
        pa.field("special_attack_stat", pa.string()),  # VARCHAR
        pa.field("special_defense_stat", pa.string()),  # VARCHAR
        pa.field("speed_stat", pa.string()),  # VARCHAR
        pa.field("types", pa.string()),  # VARCHAR
    ]
)

# Define the gold schema using PyArrow
GOLD_POKEMON_DETAILS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),  # BIGINT
        pa.field("name", pa.string()),  # VARCHAR
        pa.field("base_experience", pa.int64()),  # BIGINT
        pa.field("height", pa.int64()),  # BIGINT
        pa.field("weight", pa.int64()),  # BIGINT
        pa.field("abilities", pa.string()),  # VARCHAR
        pa.field("hp_stat", pa.string()),  # VARCHAR
        pa.field("attack_stat", pa.string()),  # VARCHAR
        pa.field("defense_stat", pa.string()),  # VARCHAR
        pa.field("special_attack_stat", pa.string()),  # VARCHAR
        pa.field("special_defense_stat", pa.string()),  # VARCHAR
        pa.field("speed_stat", pa.string()),  # VARCHAR
        pa.field("types", pa.string()),  # VARCHAR
        pa.field("capture_rate", pa.int64()),  # BIGINT
        pa.field("base_happiness", pa.int64()),  # BIGINT
        pa.field("is_baby", pa.bool_()),  # BOOLEAN
        pa.field("is_legendary", pa.bool_()),  # BOOLEAN
        pa.field("is_mythical", pa.bool_()),  # BOOLEAN
        pa.field("growth_rate", pa.string()),  # VARCHAR
        pa.field("egg_groups", pa.string()),  # VARCHAR
        pa.field("color", pa.string()),  # VARCHAR
        pa.field("shape", pa.string()),  # VARCHAR
        pa.field("evolves_from_species", pa.string()),  # VARCHAR
        pa.field("habitat", pa.string()),  # VARCHAR
        pa.field("generation", pa.string()),  # VARCHAR
        pa.field("varieties", pa.string()),  # VARCHAR
    ]
)
