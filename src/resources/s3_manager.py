import boto3
from loguru import logger


def create_s3_client(
    access_key: str, secret_key: str, endpoint_url: str
) -> boto3.client:
    """
    Creates an S3 client using the provided AWS access key, secret key, and endpoint URL.
    Args:
        access_key (str): AWS access key ID.
        secret_key (str): AWS secret access key.
        endpoint_url (str): The endpoint URL for the S3 service.
    Returns:
        boto3.client: A Boto3 S3 client object.
    """

    # Create and return your S3 client here using access_key and secret_key
    try:
        logger.info("Creating S3 client")
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        logger.success("S3 client created")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to create S3 client: {e}")
