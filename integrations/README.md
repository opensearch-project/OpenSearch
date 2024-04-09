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

##### Usage

A demo of the integration can be started using the content of this folder and Docker.

```console
docker compose -f ./docker/amazon-security-lake.yml up -d
```

This docker compose project will bring a *wazuh-indexer* node, a *wazuh-dashboard* node, 
a *logstash* node and our event generator. On the one hand, the event generator will push events 
constantly to the indexer, on the `wazuh-alerts-4.x-sample` index by default (refer to the [events 
generator](./tools/events-generator/README.md) documentation for customization options). 
On the other hand, logstash will constantly query for new data and deliver it to the integration 
Python program, also present in that node. Finally, the integration module will prepare and send the 
data to the Amazon Security Lake's S3 bucket.
<!-- TODO continue with S3 credentials setup -->

Attach a terminal to the container and start the integration by starting logstash, as follows:

```console
/usr/share/logstash/bin/logstash -f /usr/share/logstash/pipeline/indexer-to-integrator.conf --path.settings /etc/logstash
```

Unprocessed data can be sent to a file or to an S3 bucket.
```console
/usr/share/logstash/bin/logstash -f /usr/share/logstash/pipeline/indexer-to-file.conf --path.settings /etc/logstash
/usr/share/logstash/bin/logstash -f /usr/share/logstash/pipeline/indexer-to-s3.conf --path.settings /etc/logstash
```

All three pipelines are configured to fetch the latest data from the *wazuh-indexer* every minute. In
the case of `indexer-to-file`, the data is written at the same pace, whereas `indexer-to-s3`, data 
is uploaded every 5 minutes.

For development or debugging purposes, you may want to enable hot-reload, test or debug on these files, 
by using the `--config.reload.automatic`, `--config.test_and_exit` or `--debug` flags, respectively.

For production usage, follow the instructions in our documentation page about this matter.
(_when-its-done_)

As a last note, we would like to point out that we also use this Docker environment for development.

### Other integrations

TBD
