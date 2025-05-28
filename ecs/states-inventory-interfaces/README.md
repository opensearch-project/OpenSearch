| Field Name     | Type   | Description                          | Destination Field            | Custom |
| -------------- | ------ | ------------------------------------ | ---------------------------- | ------ |
| agent_id       | string | Unique ID of the agent.              | agent.id                     | FALSE  |
| agent_ip       | string | IP address of the agent.             | agent.host.ip                | TRUE   |
| agent_name     | string | Name of the agent.                   | agent.name                   | FALSE  |
| agent_version  | string | Agent version.                       | agent.version                | FALSE  |
| name           | string | Interface name.                      | interface.name               | FALSE  |
| adapter        | string | Adapter type (e.g., Ethernet, WiFi). | interface.alias              | FALSE  |
| type           | string | Network interface type.              | interface.type               | TRUE   |
| state          | string | Current state (e.g., up, down).      | interface.state              | TRUE   |
| mtu            | long   | Maximum Transmission Unit (MTU).     | interface.mtu                | TRUE   |
| mac            | string | MAC address of the interface.        | host.mac                     | FALSE  |
| tx_packets     | long   | Number of transmitted packets.       | host.network.egress.packets  | FALSE  |
| rx_packets     | long   | Number of received packets.          | host.network.ingress.packets | FALSE  |
| tx_bytes       | long   | Number of bytes transmitted.         | host.network.egress.bytes    | FALSE  |
| rx_bytes       | long   | Number of bytes received.            | host.network.ingress.bytes   | FALSE  |
| tx_errors      | long   | Number of transmission errors.       | host.network.egress.errors   | TRUE   |
| rx_errors      | long   | Number of reception errors.          | host.network.ingress.errors  | TRUE   |
| tx_dropped     | long   | Number of dropped outgoing packets.  | host.network.egress.drops    | TRUE   |
| rx_dropped     | long   | Number of dropped incoming packets.  | host.network.ingress.drops   | TRUE   |
| cluster_name   | string | Wazuh cluster name                   | wazuh.cluster.name           | TRUE   |
| cluster_node   | string | Wazuh cluster node                   | wazuh.cluster.node           | TRUE   |
| schema_version | string | Wazuh schema version                 | wazuh.schema.version         | TRUE   |
