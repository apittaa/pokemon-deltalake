INSTALL httpfs;
LOAD httpfs;

INSTALL delta;
LOAD delta;

-- Set the S3 configurations for MinIO
SET s3_url_style = 'path';  -- Use path-style access for MinIO
SET s3_endpoint = 'http://localhost:9000';  -- Endpoint of your MinIO server
SET s3_use_ssl = 'false';  -- Set to false for local MinIO without SSL
SET s3_access_key_id = 'V9FP0Kk0wCyBZ8xUQQkTJH';  -- Your access key
SET s3_secret_access_key = 'iFy6Q8kRLJW58foza0kcCEXcQkiwo09u8dtaFZhuw4J';  -- Your secret key
SET s3_region = 'us-east-1';  -- Your region

-- Create a secret with your MinIO credentials
CREATE PERSISTENT SECRET delta_s3 (
    TYPE S3,
    KEY_ID 'V9FP0Kk0wCyBZ8xUQQkTJH',  -- Your MinIO access key
    SECRET 'iFy6Q8kRLJW58foza0kcCEXcQkiwo09u8dtaFZhuw4J',  -- Your MinIO secret key
    REGION 'us-east-1',  -- Your MinIO region
    ENDPOINT 'localhost:9000',  -- Your MinIO endpoint
    URL_STYLE 'path',  -- Use path-style access for MinIO
    USE_SSL 'false'  -- Set to false for local MinIO without SSL
);


-- Try accessing the Delta Lake table stored in MinIO
SELECT * FROM delta_scan('s3://bronze/pokemons_bronze/pokemons_list/20241011_202959');

-- Pokémon species queries
SELECT * FROM delta_scan('s3://bronze/pokemons_bronze/pokemon_species/20241007_124140');

DESCRIBE
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
FROM delta_scan('s3://bronze/pokemons_bronze/pokemon_species/20241007_124140')
ORDER BY id;


-- Pokémon details queries
SELECT * FROM delta_scan('s3://bronze/pokemons_bronze/pokemon_details/20241007_124140');

DESCRIBE
SELECT
    id
    , name
    , base_experience
    , height
    , weight
    , array_to_string(json_extract_string(abilities, '$[*].ability.name'), ', ') AS abilities
    -- , json_array_length(json_extract(abilities, '$[*].ability.name')) AS ability_count
    , json_extract_string(stats, '$[0].base_stat') AS hp_stat
    , json_extract_string(stats, '$[1].base_stat') AS attack_stat
    , json_extract_string(stats, '$[2].base_stat') AS defense_stat
    , json_extract_string(stats, '$[3].base_stat') AS special_attack_stat
    , json_extract_string(stats, '$[4].base_stat') AS special_defense_stat
    , json_extract_string(stats, '$[5].base_stat') AS speed_stat
    , array_to_string(json_extract_string(types, '$[*].type.name'), ', ') AS types
    -- json_array_length(json_extract(types, '$[*].type.name')) AS type_count
FROM delta_scan('s3://bronze/pokemons_bronze/pokemon_details/20241007_124140')
ORDER BY id;

DESCRIBE
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
FROM delta_scan('s3://silver/pokemons_silver/pokemon_details/20241007_124140') pd
LEFT JOIN delta_scan('s3://silver/pokemons_silver/pokemon_species/20241007_124140') ps
ON pd.id = ps.id
ORDER BY pd.id;


CREATE OR REPLACE TABLE pokemons AS (
    SELECT
        *
    FROM delta_scan('s3://gold/pokemons_gold/pokemon_details/20241011_202959')
);

SHOW tables;

SELECT * FROM pokemons;

SELECT * FROM delta_scan('s3://gold/pokemons_gold/pokemon_details/20241007_124140');

SELECT * FROM delta_scan('s3://silver/pokemons_silver/pokemon_details/20241007_124140');

SELECT * FROM delta_scan('s3://silver/pokemons_silver/pokemon_species/20241007_124140');

SELECT * FROM read_json_auto('s3://raw/pokemons_raw/pokemons_list/20241007_124140.json');

DESCRIBE SELECT
    *
FROM read_json_auto('s3://raw/pokemons_raw/pokemons_list/20241007_124140.json');

DESCRIBE SELECT
    *
FROM read_json_auto('s3://raw/pokemons_raw/pokemon_species/20241007_124140.json', maximum_object_size=300000000);

DESCRIBE SELECT
    *
FROM read_json_auto('s3://raw/pokemons_raw/pokemon_details/20241007_124140.json', maximum_object_size=300000000);



SELECT * FROM test;

SELECT
    *
FROM read_parquet('s3://bronze/pokemons_bronze/pokemons_list/20241001_151305.parquet');
