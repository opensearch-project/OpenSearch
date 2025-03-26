| Field Name     | Type   | Description                                     | Destination Field    | Custom |
| -------------- | ------ | ----------------------------------------------- | -------------------- | ------ |
| agent_id       | string | Unique ID of the agent.                         | agent.id             | FALSE  |
| agent_ip       | string | IP address of the agent.                        | agent.host.ip        | TRUE   |
| agent_name     | string | Name of the agent.                              | agent.name           | FALSE  |
| agent_version  | string | Agent version.                                  | agent.version        | FALSE  |
| architecture   | string | Package architecture.                           | package.architecture | FALSE  |
| description    | string | Description of the package.                     | package.description  | FALSE  |
| groups         | string | Package category or group.                      | package.category     | TRUE   |
| install_time   | string | Installation timestamp.                         | package.installed    | FALSE  |
| name           | string | Package name.                                   | package.name         | FALSE  |
| location       | string | Path where the package is installed.            | package.path         | FALSE  |
| vendor         | string | Vendor or maintainer of the package.            | package.vendor       | TRUE   |
| version        | string | Package version.                                | package.version      | FALSE  |
| cluster_name   | string | Wazuh cluster name                              | wazuh.cluster.name   | TRUE   |
| cluster_node   | string | Wazuh cluster node                              | wazuh.cluster.node   | TRUE   |
| schema_version | string | Wazuh schema version                            | wazuh.schema.version | TRUE   |
|                | string | Whether the package is built for a foreign arch | packages.multiarch   | TRUE   |
|                | string | Package priority                                | packages.priority    | TRUE   |
|                | string | Package size                                    | packages.size        | FALSE  |
|                | string | Package source                                  | packages.source      | TRUE   |
|                | string | Package type                                    | packages.type        | FALSE  |
