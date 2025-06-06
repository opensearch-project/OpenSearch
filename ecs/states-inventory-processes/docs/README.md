## `wazuh-states-inventory-processes` index data model

### Fields summary

The fields are based on https://github.com/wazuh/wazuh/issues/27898

Based on ECS:

- [Agent Fields](https://www.elastic.co/guide/en/ecs/current/ecs-agent.html).
- [Process Fields](https://www.elastic.co/guide/en/ecs/current/ecs-process.html).

The detail of the fields can be found in csv file [States inventory processes Fields](fields.csv).

### Transition table

| Field Name     | Type   | Description                       | Destination Field    | Custom |
| -------------- | ------ | --------------------------------- | -------------------- | ------ |
| agent_id       | string | Unique ID of the agent.           | agent.id             | FALSE  |
| agent_ip       | string | IP address of the agent.          | agent.host.ip        | TRUE   |
| agent_name     | string | Name of the agent.                | agent.name           | FALSE  |
| agent_version  | string | Agent version.                    | agent.version        | FALSE  |
| argvs          | string | Arguments passed to the process.  | process.args         | FALSE  |
| cmd            | string | Command executed by the process.  | process.command_line | FALSE  |
| name           | string | Process name.                     | process.name         | FALSE  |
| ppid           | long   | Parent process ID.                | process.parent.pid   | FALSE  |
| pid            | string | Process ID.                       | process.pid          | FALSE  |
| state          | string | Current process state.            | process.state        | TRUE   |
| stime          | long   | System mode CPU time used.        | process.stime        | TRUE   |
| utime          | long   | User mode CPU time used.          | process.utime        | TRUE   |
| cluster_name   | string | Wazuh cluster name                | wazuh.cluster.name   | TRUE   |
| cluster_node   | string | Wazuh cluster node                | wazuh.cluster.node   | TRUE   |
| schema_version | string | Wazuh schema version              | wazuh.schema.version | TRUE   |
|                | date   | The time the process started      | process.start        | FALSE  |
|                | long   | Length of the process.args array. | process.args_count   | FALSE  |
