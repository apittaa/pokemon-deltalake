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
CREATE SECRET delta_s3 (
    TYPE S3,
    KEY_ID 'V9FP0Kk0wCyBZ8xUQQkTJH',  -- Your MinIO access key
    SECRET 'iFy6Q8kRLJW58foza0kcCEXcQkiwo09u8dtaFZhuw4J',  -- Your MinIO secret key
    REGION 'us-east-1',  -- Your MinIO region
    ENDPOINT 'localhost:9000',  -- Your MinIO endpoint
    URL_STYLE 'path',  -- Use path-style access for MinIO
    USE_SSL 'false'  -- Set to false for local MinIO without SSL
);


-- Try accessing the Delta Lake table stored in MinIO
SELECT * FROM delta_scan('s3://bronze/pokemons_bronze/pokemons_list/20241007_124140');

SELECT * FROM delta_scan('s3://bronze/pokemons_bronze/pokemon_species/20241007_124140');

SELECT * FROM delta_scan('s3://bronze/pokemons_bronze/pokemon_details/20241007_124140');



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
