import duckdb
from loguru import logger


def create_duckdb_connection(aws_access_key: str, aws_secret_access_key: str, aws_region: str) -> duckdb.DuckDBPyConnection:
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

    try:
        logger.info("Connecting to DuckDB")
        conn = duckdb.connect()
        logger.info("Installing and loading HTTPFS extension")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        logger.info("Setting S3 configuration parameters")
        conn.execute(f"SET s3_url_style='path';")
        conn.execute(f"SET s3_endpoint='s3service:9000';")
        conn.execute(f"SET s3_use_ssl = false;")
        conn.execute(f"SET s3_region='{aws_region}'")
        conn.execute(f"SET s3_access_key_id='{aws_access_key}';")
        conn.execute(f"SET s3_secret_access_key='{aws_secret_access_key}';")
        logger.info("Loading AWS credentials")
        conn.execute("CALL load_aws_credentials();")
        logger.success("Connected to DuckDB")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        return None 
    
def execute_query(conn: duckdb.DuckDBPyConnection, query: str) -> None:   
    """
    Executes a given SQL query using the provided database connection.

    Args:
        conn: A database connection object that has an execute method.
        query (str): The SQL query to be executed.

    Logs:
        Logs an info message before executing the query.
        Logs a success message if the query is executed successfully.
        Logs an error message if there is an exception during query execution.

    Raises:
        Exception: If there is an error during query execution, it is caught and logged.
    """
    try:
        logger.info(f"Executing query")
        conn.execute(query)
        logger.success(f"Query executed successfully")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
