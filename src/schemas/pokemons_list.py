import pyarrow as pa

# Define the schema using PyArrow fields for the first set of columns
POKEMONS_LIST_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string()),
        pa.field("url", pa.string()),
        pa.field("id", pa.int64()),
    ]
)
