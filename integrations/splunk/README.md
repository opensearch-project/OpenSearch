# Wazuh to Splunk Integration Developer Guide

This document describes how to prepare a Docker Compose environment to test the integration between Wazuh and Splunk. For a detailed guide on how to integrate Wazuh with Splunk, please refer to the [Wazuh documentation](https://documentation.wazuh.com/current/integrations-guide/splunk/index.html).

## Requirements

- Docker and Docker Compose installed.

## Usage

1. Clone the Wazuh repository and navigate to the `integrations/` folder.
2. Run the following command to start the environment:
   ```bash
   docker compose -f ./docker/splunk.yml up -d
   ```

The Docker Compose project will bring up the following services:

- 1x Events Generator (learn more in [wazuh-indexer/integrations/tools/events-generator](../tools/events-generator/README.md)).
- 1x Wazuh Indexer (OpenSearch).
- 1x Wazuh Dashboards (OpenSearch Dashboards).
- 1x Logstash
- 1x Splunk

For custom configurations, you may need to modify these files:

- [docker/splunk.yml](../docker/splunk.yml): Docker Compose file.
- [docker/.env](../docker/.env): Environment variables file.
- [splunk/logstash/pipeline/indexer-to-splunk.conf](./logstash/pipeline/indexer-to-splunk.conf): Logstash Pipeline configuration file.

Check the files above for **credentials**, ports, and other configurations.

| Service          | Address                | Credentials         |
| ---------------- | ---------------------- | ------------------- |
| Wazuh Indexer    | https://localhost:9200 | admin:admin         |
| Wazuh Dashboards | https://localhost:5601 | admin:admin         |
| Splunk           | https://localhost:8000 | admin:Password.1234 |

## Importing the dashboards

The dashboards for Splunk are included in this folder. The steps to import them to Splunk are the following:

- In the Splunk UI, go to `Settings` > `Data Inputs` > `HTTP Event Collector` and make sure that the `hec` token is enabled and uses the `wazuh-alerts` index. 
- Open a dashboard file and copy all its content.
- In the Splunk UI, navigate to `Search & Reporting`, `Dashboards`, click `Create New Dashboard`, write the title and select `Dashboard Studio`, select `Grid` and click on `Create`.
- On the top menu, there is a `Source` icon. Click on it, and replace all the content with the copied content from the dashboard file. After that, click on `Back` and click on `Save`.
- Repeat the steps for all the desired dashboards.

Imported dashboards will appear under `Search & Reporting` > `Dashboards`.
