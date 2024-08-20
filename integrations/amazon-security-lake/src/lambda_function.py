import logging
import os
import urllib.parse
import json
import gzip
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
import wazuh_ocsf_converter

logger = logging.getLogger()
logger.setLevel("INFO")

# Initialize boto3 client outside the handler
if os.environ.get('IS_DEV'):
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('REGION'),
        endpoint_url=os.environ.get('AWS_ENDPOINT'),
    )
else:
    s3_client = boto3.client('s3')


def get_events(bucket: str, key: str) -> list:
    """
    Retrieve events from S3 object.
    """
    logger.info(f"Reading {key}.")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = gzip.decompress(response['Body'].read()).decode('utf-8')
        return data.splitlines()
    except ClientError as e:
        logger.error(
            f"Failed to read S3 object {key} from bucket {bucket}: {e}")
        return []


def write_parquet_file(ocsf_events: list, filename: str) -> None:
    """
    Write OCSF events to a Parquet file.
    """
    table = pa.Table.from_pylist(ocsf_events)
    pq.write_table(table, filename, compression='ZSTD')


def upload_to_s3(bucket: str, key: str, filename: str) -> bool:
    """
    Upload a file to S3 bucket.
    """
    logger.info(f"Uploading data to {bucket}.")
    try:
        with open(filename, 'rb') as data:
            s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        return True
    except ClientError as e:
        logger.error(
            f"Failed to upload file {filename} to bucket {bucket}: {e}")
        return False


def exit_on_error(error_message):
    """
    Print error message and exit with non-zero status code.
    Args:
        error_message (str): Error message to display.
    """
    print(f"Error: {error_message}")
    exit(1)


def check_environment_variables(variables):
    """
    Check if required environment variables are set.
    Args:
        variables (list): List of required environment variable names.
    Returns:
        bool: True if all required environment variables are set, False otherwise.
    """
    missing_variables = [var for var in variables if not os.environ.get(var)]
    if missing_variables:
        error_message = f"The following environment variables are not set: {', '.join(missing_variables)}"
        exit_on_error(error_message)
        return False
    return True


def get_full_key(src_location: str, account_id: str, region: str, key: str, format: str) -> str:
    """
    Constructs a full S3 key path for storing a Parquet file based on event metadata.

    Args:
        src_location (str): Source location identifier.
        account_id (str): AWS account ID associated with the event.
        region (str): AWS region where the event occurred.
        key (str): Event key containing metadata information.
        format (str): File extension.

    Returns:
        str: Full S3 key path for storing the Parquet file.

    Example:
        If key is '20240417_ls.s3.0055f22e-200e-4259-b865-8ccea05812be.2024-04-17T15.45.part29.txt',
        this function will return:
        'ext/src_location/region=region/accountId=account_id/eventDay=20240417/0055f22e200e4259b8658ccea05812be.parquet'
    """
    # Extract event day from the key (first 8 characters)
    event_day = key[:8]

    # Extract filename (UUID) from the key and remove hyphens
    filename_parts = key.split('.')
    filename = ''.join(filename_parts[2].split('-'))

    # Construct the full S3 key path for storing the file
    key = (
        f'ext/{src_location}/region={region}/accountId={account_id}/eventDay={event_day}/{filename}.{format}'
    )

    return key


def lambda_handler(event, context):

    # Define required environment variables
    required_variables = ['AWS_BUCKET', 'SOURCE_LOCATION', 'ACCOUNT_ID', 'REGION']

    # Check if all required environment variables are set
    if not check_environment_variables(required_variables):
        return

    # Retrieve environment variables
    dst_bucket = os.environ['AWS_BUCKET']
    src_location = os.environ['SOURCE_LOCATION']
    account_id = os.environ['ACCOUNT_ID']
    region = os.environ['REGION']
    ocsf_bucket = os.environ.get('S3_BUCKET_OCSF')
    ocsf_class = os.environ.get('OCSF_CLASS', 'SECURITY_FINDING')

    # Extract bucket and key from S3 event
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info(f"Lambda function invoked due to {key}.")
    logger.info(
        f"Source bucket name is {src_bucket}. Destination bucket is {dst_bucket}.")

    # Read events from source S3 bucket
    raw_events = get_events(src_bucket, key)
    if not raw_events:
        return

    # Transform events to OCSF format
    ocsf_events = wazuh_ocsf_converter.transform_events(raw_events, ocsf_class)

    # Upload event in OCSF format
    ocsf_upload_success = False
    if ocsf_bucket is not None:
        tmp_filename = '/tmp/tmp.json'
        with open(tmp_filename, "w") as fd:
            fd.write(json.dumps(ocsf_events))
        ocsf_key = get_full_key(src_location, account_id, region, key, 'json')
        ocsf_upload_success = upload_to_s3(ocsf_bucket, ocsf_key, tmp_filename)

    # Write OCSF events to Parquet file
    tmp_filename = '/tmp/tmp.parquet'
    write_parquet_file(ocsf_events, tmp_filename)

    # Upload Parquet file to destination S3 bucket
    parquet_key = get_full_key(src_location, account_id, region, key, 'parquet')
    upload_success = upload_to_s3(dst_bucket, parquet_key, tmp_filename)

    # Clean up temporary file
    os.remove(tmp_filename)

    # Prepare response
    response = {
        'size': len(raw_events),
        'upload_success': upload_success,
        'ocsf_upload_success': ocsf_upload_success
    }
    return json.dumps(response)
