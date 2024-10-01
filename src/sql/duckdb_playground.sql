INSTALL httpfs;
LOAD httpfs;
SET s3_url_style='path';
SET s3_endpoint='localhost:9000';
SET s3_use_ssl='false';

CREATE SECRET s3_secret (
    TYPE S3,
    KEY_ID 'V9FP0Kk0wCyBZ8xUQQkTJH',
    SECRET 'iFy6Q8kRLJW58foza0kcCEXcQkiwo09u8dtaFZhuw4J',
    REGION 'us-east-1'
);

SELECT
    *
FROM read_json_auto('s3://raw/pokemons_raw/pokemons_list/20241001_151305.json');

SELECT
    *
FROM read_parquet('s3://bronze/pokemons_bronze/pokemons_list/20241001_151305.parquet');