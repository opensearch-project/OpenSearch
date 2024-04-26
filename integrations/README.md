## Wazuh indexer integrations

This folder contains integrations with third-party XDR, SIEM and cybersecurity software.
The goal is to transport Wazuh's analysis to the platform that suits your needs.

### Amazon Security Lake

Amazon Security Lake automatically centralizes security data from AWS environments, SaaS providers,
on premises, and cloud sources into a purpose-built data lake stored in your account. With Security Lake,
you can get a more complete understanding of your security data across your entire organization. You can
also improve the protection of your workloads, applications, and data. Security Lake has adopted the
Open Cybersecurity Schema Framework (OCSF), an open standard. With OCSF support, the service normalizes
and combines security data from AWS and a broad range of enterprise security data sources.

#### Development guide

A demo of the integration can be started using the content of this folder and Docker.

```console
docker compose -f ./docker/amazon-security-lake.yml up -d
```

This docker compose project will bring a _wazuh-indexer_ node, a _wazuh-dashboard_ node,
a _logstash_ node, our event generator and an AWS Lambda Python container. On the one hand, the event generator will push events
constantly to the indexer, to the `wazuh-alerts-4.x-sample` index by default (refer to the [events
generator](./tools/events-generator/README.md) documentation for customization options).
On the other hand, logstash will constantly query for new data and deliver it to output configured in the
pipeline, which can be one of `indexer-to-s3` or `indexer-to-file`.

The `indexer-to-s3` pipeline is the method used by the integration. This pipeline delivers
the data to an S3 bucket, from which the data is processed using a Lambda function, to finally
be sent to the Amazon Security Lake bucket in Parquet format.

<!-- TODO continue with S3 credentials setup -->

Attach a terminal to the container and start the integration by starting logstash, as follows:

```console
/usr/share/logstash/bin/logstash -f /usr/share/logstash/pipeline/indexer-to-s3.conf --path.settings /etc/logstash
```

After 5 minutes, the first batch of data will show up in http://localhost:9444/ui/wazuh-indexer-aux-bucket.
You'll need to invoke the Lambda function manually, selecting the log file to process.

```bash
bash amazon-security-lake/src/invoke-lambda.sh <file>
```

Processed data will be uploaded to http://localhost:9444/ui/wazuh-indexer-amazon-security-lake-bucket. Click on any file to download it,
and check it's content using `parquet-tools`. Just make sure of installing the virtual environment first, through [requirements.txt](./amazon-security-lake/).

```bash
parquet-tools show <parquet-file>
```

Bucket names can be configured editing the [amazon-security-lake.yml](./docker/amazon-security-lake.yml) file.

For development or debugging purposes, you may want to enable hot-reload, test or debug on these files,
by using the `--config.reload.automatic`, `--config.test_and_exit` or `--debug` flags, respectively.

For production usage, follow the instructions in our documentation page about this matter.
(_when-its-done_)

As a last note, we would like to point out that we also use this Docker environment for development.

#### Deployment guide

- Create one S3 bucket to store the raw events, for example: `wazuh-security-lake-integration`
- Create a new AWS Lambda function
  - Create an IAM role with access to the S3 bucket created above.
  - Select Python 3.12 as the runtime
  - Configure the runtime to have 512 MB of memory and 30 seconds timeout
  - Configure an S3 trigger so every created object in the bucket with `.txt` extension invokes the Lambda.
  - Run `make` to generate a zip deployment package, or create it manually as per the [AWS Lambda documentation](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html#python-package-create-dependencies).
  - Upload the zip package to the bucket. Then, upload it to the Lambda from the S3 as per these instructions: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-package.html#gettingstarted-package-zip
- Create a Custom Source within Security Lake for the Wazuh Parquet files as per the following guide: https://docs.aws.amazon.com/security-lake/latest/userguide/custom-sources.html
- Set the **AWS account ID** for the Custom Source **AWS account with permission to write data**.

<!-- TODO Configure AWS Lambda Environment Variables /-->
<!-- TODO Install and configure Logstash /-->

The instructions on this section have been based on the following AWS tutorials and documentation.

- [Tutorial: Using an Amazon S3 trigger to create thumbnail images](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-tutorial.html)
- [Tutorial: Using an Amazon S3 trigger to invoke a Lambda function](https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html)
- [Working with .zip file archives for Python Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html)
- [Best practices for working with AWS Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/best-practices.html)

### Other integrations

TBD
