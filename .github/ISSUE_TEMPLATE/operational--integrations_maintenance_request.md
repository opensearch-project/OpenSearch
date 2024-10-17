---
name: Integrations maintenance request
about: Used by the Indexer team to maintain third-party software integrations and track the results.
title: Integrations maintenance request
labels: level/task, request/operational, type/maintenance
assignees: ""
---

## Description

The Wazuh Indexer team is responsible for the maintenance of the third-party integrations hosted in the wazuh/wazuh-indexer repository. We must ensure these integrations work under new releases of the third-party software (Splunk, Elastic, Logstash, …) and our own.

For that, we need to:

-   [ ] Create a pull request that upgrades the components to the latest version.
-   [ ] Update our testing environments to verify the integrations work under new versions.
-   [ ] Test the integrations, checking that:
  - The Docker Compose project starts without errors.
  - The data arrives to the destination.
  - All the dashboards can be imported successfully.
  - All the dashboards are populated with data.
-   [ ] Finally, upgrade the compatibility matrix in integrations/README.md with the new versions.

> [!NOTE]
> * For Logstash, we use the logstash-oss image.
> * For Wazuh Indexer and Wazuh Dashboard, we use the opensearch and opensearch-dashboards images. These must match the opensearch version that we support (e.g: for Wazuh 4.9.0 it is OpenSearch 2.13.0). 

## Issues

-   _List here the detected issues_
