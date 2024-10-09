import duckdb
from loguru import logger


def create_duckdb_connection(
    aws_access_key: str, aws_secret_access_key: str, aws_region: str
) -> duckdb.DuckDBPyConnection:
    """
    Establishes a connection to DuckDB and configures it to use AWS S3 credentials.

    This function connects to DuckDB, installs and loads the HTTPFS extension, and sets various S3-related
    configuration parameters including the AWS access key, secret access key, and region. It then attempts
    to load the AWS credentials into the DuckDB session.

    Args:
        aws_access_key (str): AWS access key ID.
        aws_secret_access_key (str): AWS secret access key.
        aws_region (str): AWS region.

    Returns:
        duckdb.DuckDBPyConnection: A DuckDB connection object if the connection is successful, otherwise None.

    Raises:
        Exception: If there is an error connecting to DuckDB or setting the S3 configuration parameters.
    """

    # Connect to DuckDB and configure it to use AWS S3 credentials
    try:
        logger.info("Connecting to DuckDB")
        conn = duckdb.connect()
        logger.info("Installing and loading HTTPFS extension")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        logger.info("Creating and using S3 secret")
        conn.execute(f"""
            CREATE SECRET delta_s3 (
                TYPE S3,
                KEY_ID '{aws_access_key}',  -- Your AWS access key
                SECRET '{aws_secret_access_key}',  -- Your AWS secret key
                REGION '{aws_region}',  -- Your AWS region
                ENDPOINT 'localhost:9000',  -- Your S3 endpoint
                URL_STYLE 'path',  -- Use path-style access for S3
                USE_SSL 'false'  -- Set to false for local S3 without SSL
            );
        """)
        logger.success("Connected to DuckDB")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        return None


def execute_query(
    conn: duckdb.DuckDBPyConnection, query: str
) -> duckdb.DuckDBPyRelation:
    """
    Executes a given SQL query using the provided database connection and returns the result.

    Args:
        conn: A database connection object that has an execute method.
        query (str): The SQL query to be executed.

    Returns:
        duckdb.DuckDBPyRelation: The result of the query execution.

    Logs:
        Logs an info message before executing the query.
        Logs a success message if the query is executed successfully.
        Logs an error message if there is an exception during query execution.

    Raises:
        Exception: If there is an error during query execution, it is caught and logged.
    """

    # Execute the query and return the result
    try:
        logger.info("Executing query")
        result = conn.execute(query)
        logger.success("Query executed successfully")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return None
