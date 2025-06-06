## `wazuh-states-inventory-ports` index data model

### Fields summary

The fields are based on https://github.com/wazuh/wazuh/issues/27898

Based on ECS:

- [Agent Fields](https://www.elastic.co/guide/en/ecs/current/ecs-agent.html).
- [Destination Fields](https://www.elastic.co/guide/en/ecs/current/ecs-destination.html).
- [Source Fields](https://www.elastic.co/guide/en/ecs/current/ecs-source.html).
- [File Fields](https://www.elastic.co/guide/en/ecs/current/ecs-file.html).
- [Interface Fields](https://www.elastic.co/guide/en/ecs/current/ecs-interface.html).
- [Network Fields](https://www.elastic.co/guide/en/ecs/current/ecs-network.html).
- [Process Fields](https://www.elastic.co/guide/en/ecs/current/ecs-process.html).

The detail of the fields can be found in csv file [States inventory ports Fields](fields.csv).

### Transition table

| Field Name     | Type   | Description                                   | Destination Field          | Custom |
|----------------|--------|-----------------------------------------------|----------------------------|--------|
| agent_id       | string | Unique ID of the agent.                       | agent.id                   | FALSE  |
| agent_ip       | string | IP address of the agent.                      | agent.host.ip              | TRUE   |
| agent_name     | string | Name of the agent.                            | agent.name                 | FALSE  |
| agent_version  | string | Agent version.                                | agent.version              | FALSE  |
| local_ip       | string | Local IP address.                             | destination.ip             | FALSE  |
| local_port     | long   | Local port number.                            | destination.port           | FALSE  |
| inode          | long   | Inode associated with the connection.         | file.inode                 | FALSE  |
| tx_queue       | long   | Transmit queue length.                        | host.network.egress.queue  | TRUE   |
| rx_queue       | long   | Receive queue length.                         | host.network.ingress.queue | FALSE  |
| state          | string | Connection state (e.g., LISTEN, ESTABLISHED). | interface.state            | TRUE   |
| protocol       | string | Transport protocol (TCP/UDP).                 | network.transport          | FALSE  |
| process        | string | Name of the process using the port.           | process.name               | FALSE  |
| pid            | long   | Process ID using the port.                    | process.pid                | FALSE  |
| remote_ip      | string | Remote IP address.                            | source.ip                  | FALSE  |
| remote_port    | long   | Remote port number.                           | source.port                | FALSE  |
| cluster_name   | string | Wazuh cluster name                            | wazuh.cluster.name         | TRUE   |
| cluster_node   | string | Wazuh cluster node                            | wazuh.cluster.node         | TRUE   |
| schema_version | string | Wazuh schema version                          | wazuh.schema.version       | TRUE   |
