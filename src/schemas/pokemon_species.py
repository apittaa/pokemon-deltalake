import pyarrow as pa

# Define the schema using PyArrow
POKEMON_SPICIES_SCHEMA = pa.schema(
    [
        ("base_happiness", pa.int64()),
        ("capture_rate", pa.int64()),
        ("color", pa.struct([("name", pa.string()), ("url", pa.string())])),
        (
            "egg_groups",
            pa.list_(pa.struct([("name", pa.string()), ("url", pa.string())])),
        ),
        ("evolution_chain", pa.struct([("url", pa.string())])),
        (
            "evolves_from_species",
            pa.struct([("name", pa.string()), ("url", pa.string())]),
        ),
        (
            "flavor_text_entries",
            pa.list_(
                pa.struct(
                    [
                        ("flavor_text", pa.string()),
                        (
                            "language",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
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
            "form_descriptions",
            pa.list_(
                pa.struct(
                    [
                        ("description", pa.string()),
                        (
                            "language",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        ("forms_switchable", pa.bool_()),
        ("gender_rate", pa.int64()),
        (
            "genera",
            pa.list_(
                pa.struct(
                    [
                        ("genus", pa.string()),
                        (
                            "language",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        ("generation", pa.struct([("name", pa.string()), ("url", pa.string())])),
        ("growth_rate", pa.struct([("name", pa.string()), ("url", pa.string())])),
        ("habitat", pa.struct([("name", pa.string()), ("url", pa.string())])),
        ("has_gender_differences", pa.bool_()),
        ("hatch_counter", pa.int64()),
        ("id", pa.int64()),
        ("is_baby", pa.bool_()),
        ("is_legendary", pa.bool_()),
        ("is_mythical", pa.bool_()),
        ("name", pa.string()),
        (
            "names",
            pa.list_(
                pa.struct(
                    [
                        (
                            "language",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                        ("name", pa.string()),
                    ]
                )
            ),
        ),
        ("order", pa.int64()),
        (
            "pal_park_encounters",
            pa.list_(
                pa.struct(
                    [
                        (
                            "area",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                        ("base_score", pa.int64()),
                        ("rate", pa.int64()),
                    ]
                )
            ),
        ),
        (
            "pokedex_numbers",
            pa.list_(
                pa.struct(
                    [
                        ("entry_number", pa.int64()),
                        (
                            "pokedex",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
        ("shape", pa.struct([("name", pa.string()), ("url", pa.string())])),
        (
            "varieties",
            pa.list_(
                pa.struct(
                    [
                        ("is_default", pa.bool_()),
                        (
                            "pokemon",
                            pa.struct([("name", pa.string()), ("url", pa.string())]),
                        ),
                    ]
                )
            ),
        ),
    ]
)
