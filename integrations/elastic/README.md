# Wazuh to Elastic Integration Developer Guide

This document describes how to prepare a Docker Compose environment to test the integration between Wazuh and the Elastic Stack. For a detailed guide on how to integrate Wazuh with Elastic Stack, please refer to the [Wazuh documentation](https://documentation.wazuh.com/current/integrations-guide/elastic-stack/index.html).

## Requirements

- Docker and Docker Compose installed.

## Usage

1. Clone the Wazuh repository and navigate to the `integrations/` folder.
2. Run the following command to start the environment:
   ```bash
   docker compose -f ./docker/elastic.yml up -d
   ```

The Docker Compose project will bring up the following services:

- 1x Events Generator (learn more in [wazuh-indexer/integrations/tools/events-generator](../tools/events-generator/README.md)).
- 1x Wazuh Indexer (OpenSearch).
- 1x Wazuh Dashboards (OpenSearch Dashboards).
- 1x Logstash
- 1x Elastic
- 1x Kibana

For custom configurations, you may need to modify these files:

- [docker/elastic.yml](../docker/elastic.yml): Docker Compose file.
- [docker/.env](../docker/.env): Environment variables file.
- [elastic/logstash/pipeline/indexer-to-elastic.conf](./logstash/pipeline/indexer-to-elastic.conf): Logstash Pipeline configuration file.

Check the files above for **credentials**, ports, and other configurations.

| Service          | Address                | Credentials     |
| ---------------- | ---------------------- | --------------- |
| Wazuh Indexer    | https://localhost:9200 | admin:admin     |
| Wazuh Dashboards | https://localhost:5601 | admin:admin     |
| Elastic          | https://localhost:9201 | elastic:elastic |
| Kibana           | https://localhost:5602 | elastic:elastic |

## Importing the dashboards

The dashboards for Elastic are included in [dashboards.ndjson](./dashboards.ndjson). The steps to import them to Elastic are the following:

- On Kibana, expand the left menu, and go to `Stack management`.
- Click on `Saved Objects`, select `Import`, click on the `Import` icon and browse the dashboard file.
- Click on Import and complete the process.

Imported dashboards will appear in the `Dashboards` app on the left menu.
