# Wazuh to Amazon Security Lake Integration Development Guide

## Deployment guide on Docker

A demo of the integration can be started using the content of this folder and Docker. Open a terminal in the `wazuh-indexer/integrations` folder and start the environment.

```console
docker compose -f ./docker/compose.amazon-security-lake.yml up -d
```

This Docker Compose project will bring up these services:

- a _wazuh-indexer_ node
- a _wazuh-dashboard_ node
- a _logstash_ node
- our [events generator](../tools/events-generator/README.md)
- an AWS Lambda Python container.

| Service       | Address                  | Credentials     |
| ------------- | ------------------------ | --------------- |
| Wazuh Indexer | https://localhost:9200   | admin:admin     |
| Dashboards    | https://localhost:5601   | admin:admin     |
| S3 Ninja      | http://localhost:9444/ui |                 |

On the one hand, the event generator will push events constantly to the indexer, to the `wazuh-alerts-4.x-sample` index by default (refer to the [events generator](../tools/events-generator/README.md) documentation for customization options). On the other hand, Logstash will query for new data and deliver it to output configured in the pipeline `indexer-to-s3`. This pipeline delivers the data to an S3 bucket, from which the data is processed using a Lambda function, to finally be sent to the Amazon Security Lake bucket in Parquet format.

The pipeline starts automatically, but if you need to start it manually, attach a terminal to the Logstash container and start the integration using the command below:

```console
/usr/share/logstash/bin/logstash -f /usr/share/logstash/pipeline/indexer-to-s3.conf
```

After 5 minutes, the first batch of data will show up in http://localhost:9444/ui/wazuh-aws-security-lake-raw. You'll need to invoke the Lambda function manually, selecting the log file to process.

```bash
bash amazon-security-lake/invoke-lambda.sh <file>
```

Processed data will be uploaded to http://localhost:9444/ui/wazuh-aws-security-lake-parquet. Click on any file to download it, and check it's content using `parquet-tools`. Just make sure of installing the virtual environment first, through [requirements.txt](./requirements.txt).

```bash
parquet-tools show <parquet-file>
```

If the `S3_BUCKET_OCSF` variable is set in the container running the AWS Lambda function, intermediate data in OCSF and JSON format will be written to a dedicated bucket. This is enabled by default, writing to the `wazuh-aws-security-lake-ocsf` bucket. Bucket names and additional environment variables can be configured editing the [compose.amazon-security-lake.yml](../docker/compose.amazon-security-lake.yml) file.

For development or debugging purposes, you may want to enable hot-reload, test or debug on these files, by using the `--config.reload.automatic`, `--config.test_and_exit` or `--debug` flags, respectively.

For production usage, follow the instructions in our documentation page about this matter.
See [README.md](README.md). The instructions on that section have been based on the following AWS tutorials and documentation.

- [Tutorial: Using an Amazon S3 trigger to create thumbnail images](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-tutorial.html)
- [Tutorial: Using an Amazon S3 trigger to invoke a Lambda function](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html)
- [Working with .zip file archives for Python Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html)
- [Best practices for working with AWS Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)

## Makefile

**Docker is required**.

The [Makefile](./Makefile) in this folder automates the generation of a zip deployment package containing the source code and the required dependencies for the AWS Lambda function. Simply run `make` and it will generate the `wazuh_to_amazon_security_lake.zip` file. The main target runs a Docker container to install the Python3 dependencies locally, and zips the source code and the dependencies together.
