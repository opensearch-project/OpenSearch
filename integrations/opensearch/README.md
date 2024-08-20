# Wazuh to OpenSearch Integration Developer Guide

This document describes how to prepare a Docker Compose environment to test the integration between Wazuh and the OpenSearch Stack. For a detailed guide on how to integrate Wazuh with OpenSearch Stack, please refer to the [Wazuh documentation](https://documentation.wazuh.com/current/integrations-guide/OpenSearch-stack/index.html).

## Requirements

- Docker and Docker Compose installed.

## Usage

1. Clone the Wazuh repository and navigate to the `integrations/` folder.
2. Run the following command to start the environment:
   ```bash
   docker compose -f ./docker/compose.indexer-opensearch.yml up -d
   ```
3. If you prefer, you can start the integration with the Wazuh Manager as data source:
   ```bash
   docker compose -f ./docker/compose.manager-opensearch.yml up -d
   ```

The Docker Compose project will bring up the following services:

- 1x Events Generator (learn more in [wazuh-indexer/integrations/tools/events-generator](../tools/events-generator/README.md)).
- 1x Wazuh Indexer (OpenSearch).
- 1x Logstash
- 1x OpenSearch
- 1x OpenSearch Dashboards
- 1x Wazuh Manager (optional).

For custom configurations, you may need to modify these files:

- [docker/compose.indexer-opensearch.yml](../docker/compose.indexer-opensearch.yml): Docker Compose file.
- [docker/.env](../docker/.env): Environment variables file.
- [opensearch/logstash/pipeline/indexer-to-opensearch.conf](./logstash/pipeline/indexer-to-opensearch.conf): Logstash Pipeline configuration file.

If you opted to start the integration with the Wazuh Manager, you can modify the following files:

- [docker/compose.manager-opensearch.yml](../docker/compose.manager-opensearch.yml): Docker Compose file.
- [opensearch/logstash/pipeline/manager-to-opensearch.conf](./logstash/pipeline/manager-to-opensearch.conf): Logstash Pipeline configuration file.

Check the files above for **credentials**, ports, and other configurations.

| Service               | Address                | Credentials |
| --------------------- | ---------------------- | ----------- |
| Wazuh Indexer         | https://localhost:9200 | admin:admin |
| OpenSearch            | https://localhost:9201 | admin:admin |
| OpenSearch Dashboards | https://localhost:5602 | admin:admin |

## Importing the dashboards

The dashboards for OpenSearch are included in [dashboards.ndjson](./dashboards.ndjson). The steps to import them to OpenSearch are the following:

- On OpenSearch Dashboards, expand the left menu, and go to `Dashboards Management`.
- Click on `Saved Objects`, select `Import`, click on the `Import` icon and browse the dashboard file.
- Click on Import and complete the process.

Imported dashboards will appear in the `Dashboards` app on the left menu.
