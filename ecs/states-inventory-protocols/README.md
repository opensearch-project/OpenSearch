| Field Name     | Type   | Description                            | Destination Field    | Custom |
| -------------- | ------ | -------------------------------------- | -------------------- | ------ |
| agent_id       | string | Unique ID of the agent.                | agent.id             | FALSE  |
| agent_ip       | string | IP address of the agent.               | agent.host.ip        | TRUE   |
| agent_name     | string | Name of the agent.                     | agent.name           | FALSE  |
| agent_version  | string | Agent version.                         | agent.version        | FALSE  |
| iface          | string | Interface name.                        | interface.name       | FALSE  |
| type           | string | Protocol type (e.g., static, dynamic). | network.type         | FALSE  |
| gateway        | string | Default gateway address.               | network.gateway      | TRUE   |
| dhcp           | bool   | Indicates if DHCP is used (yes/no).    | network.dhcp         | TRUE   |
| metric         | string | Routing metric value.                  | network.metric       | TRUE   |
| cluster_name   | string | Wazuh cluster name                     | wazuh.cluster.name   | TRUE   |
| cluster_node   | string | Wazuh cluster node                     | wazuh.cluster.node   | TRUE   |
| schema_version | string | Wazuh schema version                   | wazuh.schema.version | TRUE   |
