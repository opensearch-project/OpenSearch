## `wazuh-states-inventory-hardware` index data model

### Fields summary

The fields are based on https://github.com/wazuh/wazuh/issues/27898

Based on ECS:

- [Agent Fields](https://www.elastic.co/guide/en/ecs/current/ecs-agent.html).
- [Host Fields](https://www.elastic.co/guide/en/ecs/current/ecs-host.html).

The detail of the fields can be found in csv file [States inventory hardware Fields](fields.csv).

### Transition table

| Field Name     | Type         | Description                           | Destination Field    | Custom |
| -------------- | ------------ | ------------------------------------- | -------------------- | ------ |
| agent_id       | string       | Unique ID of the agent.               | agent.id             | FALSE  |
| agent_ip       | string       | IP address of the agent.              | agent.host.ip        | TRUE   |
| agent_name     | string       | Name of the agent.                    | agent.name           | FALSE  |
| agent_version  | string       | Agent version.                        | agent.version        | FALSE  |
| board_serial   | string       | Serial Number of the device.          | host.serial_number   | TRUE   |
| cpu_name       | string       | Name/model of the CPU.                | host.cpu.name        | TRUE   |
| cpu_cores      | long         | Number of CPU cores.                  | host.cpu.cores       | TRUE   |
| cpu_mhz        | double       | CPU clock speed in MHz.               | host.cpu.speed       | TRUE   |
| ram_total      | long         | Total memory, in Bytes.               | host.memory.total    | TRUE   |
| ram_free       | long         | Free memory, in Bytes.                | host.memory.free     | TRUE   |
| -              | long         | Used memory, in Bytes.                | host.memory.used     | TRUE   |
| ram_usage      | scaled_float | Percent memory used, between 0 and 1. | host.memory.usage    | TRUE   |
| cluster_name   | string       | Wazuh cluster name                    | wazuh.cluster.name   | TRUE   |
| cluster_node   | string       | Wazuh cluster node                    | wazuh.cluster.node   | TRUE   |
| schema_version | string       | Wazuh schema version                  | wazuh.schema.version | TRUE   |
