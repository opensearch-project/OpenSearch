| Field Name         | Type   | Description                                                                       | Destination Field            | Custom |
| ------------------ | ------ | --------------------------------------------------------------------------------- | ---------------------------- | ------ |
| agent_id           | string | Unique ID of the agent.                                                           | agent.id                     | FALSE  |
| agent_ip           | string | IP address of the agent.                                                          | agent.host.ip                | TRUE   |
| agent_name         | string | Name of the agent.                                                                | agent.name                   | FALSE  |
| agent_version      | string | Agent version.                                                                    | agent.version                | FALSE  |
| hostname           | string | System hostname.                                                                  | host.hostname                | FALSE  |
| architecture       | string | CPU architecture (e.g., x86_64, ARM).                                             | host.architecture            | FALSE  |
| os_name            | string | Operating system name.                                                            | host.os.name                 | FALSE  |
| os_version         | string | Full OS version.                                                                  | host.os.version              | FALSE  |
| os_platform        | string | Platform name (e.g., Debian, RedHat).                                             | host.os.platform             | FALSE  |
| os_display_version | string | Human-readable OS version.                                                        | host.os.full                 | FALSE  |
| os_codename        | string | OS codename (if applicable).                                                      | host.os.codename             | TRUE   |
| os_major           | string | Major version number.                                                             | host.os.major                | TRUE   |
| os_minor           | string | Minor version number.                                                             | host.os.minor                | TRUE   |
| os_patch           | string | Patch level of the OS.                                                            | host.os.patch                | TRUE   |
| os_build           | string | Build number of the OS.                                                           | host.os.build                | TRUE   |
| sysname            | string | System kernel name.                                                               | host.os.kernel.name          | TRUE   |
| release            | string | Kernel release version.                                                           | host.os.kernel.release       | TRUE   |
| version            | string | Kernel version.                                                                   | host.os.kernel.version       | TRUE   |
| os_release         | string | Distribution-specific release information.                                        | host.os.distribution.release | TRUE   |
| cluster_name       | string | Wazuh cluster name                                                                | wazuh.cluster.name           | TRUE   |
| cluster_node       | string | Wazuh cluster node                                                                | wazuh.cluster.node           | TRUE   |
| schema_version     | string | Wazuh schema version                                                              | wazuh.schema.version         | TRUE   |
|                    | string | Which commercial OS family (one of: linux, macos, unix, windows, ios or android). | host.os.type                 | FALSE  |
