OpenSearch Upgrade CLI
===

The `opensearch-upgrade` tool included in the distribution automates some of these steps in [Upgrade to OpenSearch](https://opensearch.org/docs/upgrade-to/upgrade-to/#upgrade-to-opensearch) to make this process easier, consistent and error-free. It provides the following features,
* imports the existing configurations and applies it to the new installation of OpenSearch.
* installs the existing core plugins.

However, **please note that**
* The tool doesn't perform an end-to-end upgrade but rather assists during an upgrade process. It must be run on each node of the cluster individually as part of the upgrade process.
* The tool doesn't provide a rollback option once you've upgraded a node. So make sure you follow best practices and take backups.
* All community plugins must be installed (if available) manually.
* The keystore settings can only be validated at service start up time, so any unsupported setting that is imported must be removed manually for the service to start.

Usage
---
The following instructions are for performing a rolling upgrade using the OpenSearch tarball distribution. Please refer to [Upgrade paths](https://opensearch.org/docs/upgrade-to/upgrade-to/#upgrade-paths) to make sure that the upgrade is supported and whether you need to upgrade to a supported Elasticsearch OSS version first.

Then, follow the below steps,

1. Disable shard allocation to prevent Elasticsearch OSS from replicating shards as you shut down nodes:
   ```
   PUT _cluster/settings
   {
     "persistent": {
         "cluster.routing.allocation.enable": "primaries"
      }
   }
   ```
2. On one of the nodes, [Download](https://opensearch.org/downloads.html) and extract the OpenSearch tarball to a new directory.
3. Make sure the following environment variables are set,
   * `ES_HOME` - path to existing Elasticsearch installation home
   * `ES_PATH_CONF` - path to the config directory of existing Elasticsearch installation
   * `OPENSEARCH_HOME` - path to the OpenSearch installation home
   * `OPENSEARCH_PATH_CONF` - path to the OpenSearch config directory
4. The `opensearch-upgrade` is included in the `bin` directory in the distribution. This must be run by the same user running the current Elasticsearch service. Run it by issuing the command from the distribution home.
    ```
    ./bin/opensearch-upgrade
    ```
5. Stop the running Elasticsearch OSS on the node. On Linux distributions that use systemd, use this command:
    ```
    sudo systemctl stop elasticsearch.service
    ```
   For tarball installations, find the process ID (`ps aux`) and kill it (`kill <pid>`).
6. Start OpenSearch on the node (`./bin/opensearch -d.`)
7. Repeat the step 2-6 for all other remaining nodes in the cluster.
8. Once all nodes are upgraded to OpenSearch, re-enable shard allocation.
   ```
    PUT _cluster/settings
    {
      "persistent": {
        "cluster.routing.allocation.enable": "all"
      }
    }
   ```

About the tool
---
The tool sequentially performs the following tasks,

1. Looks for a valid Elasticsearch installation on the current node. It reads the `elasticsearch.yml` to get the endpoint details to connect to the locally running Elasticsearch service. If it is unable to do so, it tries to retrieve the information from the `ES_HOME` location.
2. Verifies if the existing version of Elasticsearch is compatible with the OpenSearch version. It prints a summary of the information gathered to the user and prompts for confirmation to proceed.
3. Imports the settings from the `elasticsearch.yml` config file into the `opensearch.yml` config file.
4. Copies across any custom JVM options from the `$ES_PATH_CONF/jvm.options.d` directory into the `$OPENSEARCH_PATH_CONF/jvm.options.d`. Similarly, It also imports the logging configurations from the `$ES_PATH_CONF/log4j2.properties` into `$OPENSEARCH_PATH_CONF/log4j2.properties`.
5. Installs the core plugins that are currently installed in the `$ES_HOME/plugins` directory. *All other third party community plugins must be installed manually.*
6. Imports the secure settings from the `elasticsearch.keystore` (if any) into the `opensearch.keystore`. If the keystore file is password protected, it prompts the user to enter the password.
