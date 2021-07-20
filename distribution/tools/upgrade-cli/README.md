OpenSearch Upgrade CLI
===

While upgrading to OpenSearch from a compatible version of Elasticsearch, there are some manual tasks that need to be performed.
The `opensearch-upgrade` tool included in the distribution automates those tasks to make the upgrade process easier, consistent and error-free.
It provides the following features,
* imports the existing configurations and applies it to the new installation of OpenSearch.
* installs the existing core plugins.

However, **please note that**
* The tool doesn't perform an end-to-end upgrade but rather assists during an upgrade process. Stopping of the existing service and starting the OpenSearch service must be done manually. It must be run on each node of the cluster individually as part of the upgrade process.
* The tool doesn't provide a rollback option once you've upgraded a node. So make sure you follow best practices and take backups.
* All community plugins must be installed (if available) manually.
* The keystore settings can only be validated at service start up time, so any unsupported setting that is imported must be removed manually for the service to start.

Usage
---
Make sure the following environment variables are available in the current environment

* `ES_HOME` - path to existing Elasticsearch installation home
* `ES_PATH_CONF` - path to the config directory of existing Elasticsearch installation
* `OPENSEARCH_HOME` - path to the OpenSearch installation home
* `OPENSEARCH_PATH_CONF` - path to the OpenSearch config directory

The `opensearch-upgrade` is included in the `bin` directory. This must be run by the same user running the current Elasticsearch service. On each node of the cluster, the tool can be run by issuing the command.

```
$OPENSEARCH_HOME/bin/opensearch-upgrade
```

The tool sequentially performs the following tasks,

1. Looks for a valid Elasticsearch installation on the current node. It reads the `elasticsearch.yml` to get the endpoint details to connect to the locally running Elasticsearch service. If it is unable to do so, it tries to retrieve the information from the `ES_HOME` location.
2. Verifies if the existing version of Elasticsearch is compatible with the OpenSearch version. It prints a summary of the information gathered to the user and prompts for confirmation to proceed.
3. Imports the settings from the `elasticsearch.yml` config file into the `opensearch.yml` config file.
4. Copies across any custom JVM options from the `$ES_PATH_CONF/jvm.options.d` directory into the `$OPENSEARCH_PATH_CONF/jvm.options.d`. Similarly, It also imports the logging configurations from the `$ES_PATH_CONF/log4j2.properties` into `$OPENSEARCH_PATH_CONF/log4j2.properties`.
5. Installs the core plugins that are currently installed in the `$ES_HOME/plugins` directory. It only installs the official OpenSearch plugins. *All other third party community plugins must be installed manually.*
6. Imports the secure settings from the `elasticsearch.keystore` (if any) into the `opensearch.keystore`. If the keystore file is password protected, it prompts the user to enter the password.
