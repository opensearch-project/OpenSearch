#!/env/bin/python3
# vim: bkc=yes bk wb

import sys
import os
import datetime
import transform
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import boto3
from botocore.exceptions import ClientError

# NOTE work in progress
def upload_file(table, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param table: PyArrow table with events data
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    client = boto3.client(
        service_name='s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name=os.environ['AWS_REGION'],
        endpoint_url='http://s3.ninja:9000',
    )

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    try:
        client.put_object(Bucket=bucket, Key=file_name, Body=open(file_name, 'rb'))
    except ClientError as e:
        logging.error(e)
        return False
    return True


def main():
    '''Main function'''
    # Get current timestamp
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Generate filenames
    filename_raw = f"/tmp/integrator-raw-{timestamp}.json"
    filename_ocsf = f"/tmp/integrator-ocsf-{timestamp}.json"
    filename_parquet = f"/tmp/integrator-ocsf-{timestamp}.parquet"

    # 1. Extract data
    #    ================
    raw_data = []
    for line in sys.stdin:
        raw_data.append(line)

        # Echo piped data
        with open(filename_raw, "a") as fd:
            fd.write(line)

    # 2. Transform data
    #    ================
    # a. Transform to OCSF
    ocsf_data = []
    for line in raw_data:
        try:
            event = transform.converter.from_json(line)
            ocsf_event = transform.converter.to_detection_finding(event)
            ocsf_data.append(ocsf_event.model_dump())

            # Temporal disk storage
            with open(filename_ocsf, "a") as fd:
                fd.write(str(ocsf_event) + "\n")
        except AttributeError as e:
            print("Error transforming line to OCSF")
            print(event)
            print(e)

    # b. Encode as Parquet
    try:
        table = pa.Table.from_pylist(ocsf_data)
        pq.write_table(table, filename_parquet)
    except AttributeError as e:
        print("Error encoding data to parquet")
        print(e)

    # 3. Load data (upload to S3)
    #    ================
    if upload_file(table, filename_parquet, os.environ['AWS_BUCKET']):
        # Remove /tmp files
        pass


def _test():
    ocsf_event = {}
    with open("./wazuh-event.sample.json", "r") as fd:
        # Load from file descriptor
        for raw_event in fd:
            try:
                event = transform.converter.from_json(raw_event)
                print("")
                print("-- Wazuh event Pydantic model")
                print("")
                print(event.model_dump())
                ocsf_event = transform.converter.to_detection_finding(event)
                print("")
                print("-- Converted to OCSF")
                print("")
                print(ocsf_event.model_dump())

            except KeyError as e:
                raise (e)


if __name__ == '__main__':
    main()
    # _test()
