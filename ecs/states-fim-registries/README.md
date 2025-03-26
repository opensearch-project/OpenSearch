| Field Name     | Type   | Description                                         | Destination Field         | Custom |
| -------------- | ------ | :-------------------------------------------------- | ------------------------- | ------ |
| agent_id       | string | Unique identifier of the agent, e.g., "001".        | agent.id                  | FALSE  |
| agent_ip       | string | IP address of the agent.                            | agent.host.ip             | TRUE   |
| agent_name     | string | Name assigned to the agent.                         | agent.name                | FALSE  |
| agent_version  | string | Version of the agent software, e.g., "v4.10.2".     | agent.version             | FALSE  |
| arch           | string | Registry architecture type, e.g., "[x86]", "[x64]". | agent.host.architecture   | TRUE   |
| cluster_name   | string | Wazuh cluster name                                  | wazuh.cluster.name        | TRUE   |
| cluster_node   | string | Wazuh cluster node                                  | wazuh.cluster.node        | TRUE   |
|                | string | Architecture associated with the entity             | registry.architecture     | TRUE   |
| gid            | string | Group ID associated with the entity.                | registry.gid              | TRUE   |
| group_name     | string | Name of the group that owns the entity.             | registry.group            | TRUE   |
| hash_md5       | string | MD5 hash of the file or registry value content.     | registry.data.hash.md5    | TRUE   |
| hash_sha1      | string | SHA-1 hash of the file or registry value content.   | registry.data.hash.sha1   | TRUE   |
| hash_sha256    | string | SHA-256 hash of the file or registry value content. | registry.data.hash.sha256 | TRUE   |
| hive           | string | Abbreviated name for the hive.                      | registry.hive             | FALSE  |
| key            | string | Hive-relative path of keys                          | registry.key              | FALSE  |
| mtime          | long   | Last modified timestamp of the entity.              | registry.mtime            | TRUE   |
| path           | string | Absolute file path or full registry key path.       | registry.path             | FALSE  |
| schema_version | string | Wazuh schema version                                | wazuh.schema.version      | TRUE   |
| size           | long   | Size of the file or registry value (in bytes).      | registry.size             | TRUE   |
| timestamp      | long   | Timestamp when the event was generated.             | timestamp                 | FALSE  |
| type           | string | Type of monitored entity, e.g., "registry_key".     | event.category            | FALSE  |
| uid            | string | User ID associated with the entity.                 | registry.uid              | TRUE   |
| user_name      | string | Name of the owner of the entity (user).             | registry.owner            | TRUE   |
| value_name     | string | Name of the registry value.                         | registry.value            | FALSE  |
| value_type     | string | Type of the registry value, e.g., "REG_SZ".         | registry.data.type        | FALSE  |
