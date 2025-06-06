## `wazuh-states-vulnerabilities` index data model

### Fields summary

The fields are based on https://github.com/wazuh/wazuh/issues/27898

Based on ECS:

- [Agent Fields](https://www.elastic.co/guide/en/ecs/current/ecs-agent.html).
- [Package Fields](https://www.elastic.co/guide/en/ecs/current/ecs-package.html).
- [Host Fields](https://www.elastic.co/guide/en/ecs/current/ecs-host.html).
- [Operating System Fields](https://www.elastic.co/guide/en/ecs/current/ecs-os.html).
- [Vulnerability Fields](https://www.elastic.co/guide/en/ecs/current/ecs-vulnerability.html).

The detail of the fields can be found in csv file [States vulnerabilities Fields](fields.csv).

### Transition table

| Field Name                        | Type    | Description                                                                                                         | Destination Field                 | Custom |
|-----------------------------------|---------|---------------------------------------------------------------------------------------------------------------------|-----------------------------------|--------|
| agent.build.original              | keyword | Extended build information for the agent.                                                                           | agent.build.original              | TRUE   |
| agent.ephemeral_id                | keyword | Ephemeral identifier of this agent.                                                                                 | agent.ephemeral_id                | TRUE   |
| agent.id                          | keyword | Unique identifier of this agent.                                                                                    | agent.id                          | TRUE   |
| agent.name                        | keyword | Custom name of the agent.                                                                                           | agent.name                        | TRUE   |
| agent.type                        | keyword | Type of the agent.                                                                                                  | agent.type                        | TRUE   |
| agent.version                     | keyword | Version of the agent.                                                                                               | agent.version                     | TRUE   |
| host.os.full                      | keyword | Operating system name, including the version or code name.                                                          | host.os.full                      | TRUE   |
| host.os.full.text                 | keyword | Operating system name, including the version or code name.                                                          | host.os.full.text                 | TRUE   |
| host.os.kernel                    | keyword | Operating system kernel version as a raw string.                                                                    | host.os.kernel                    | TRUE   |
| host.os.name                      | keyword | Operating system name, without the version.                                                                         | host.os.name                      | TRUE   |
| host.os.name.text                 | keyword | Operating system name, without the version.                                                                         | host.os.name.text                 | TRUE   |
| host.os.platform                  | keyword | Operating system platform (such as centos, ubuntu, windows).                                                        | host.os.platform                  | TRUE   |
| host.os.type                      | keyword | Which commercial OS family (one of: linux, macos, unix, windows, ios or android).                                   | host.os.type                      | TRUE   |
| host.os.version                   | keyword | Operating system version as a raw string.                                                                           | host.os.version                   | TRUE   |
| package.architecture              | keyword | Package architecture.                                                                                               | package.architecture              | TRUE   |
| package.build_version             | keyword | Build version information.                                                                                          | package.build_version             | TRUE   |
| package.checksum                  | keyword | Checksum of the installed package for verification.                                                                 | package.checksum                  | TRUE   |
| package.description               | keyword | Description of the package.                                                                                         | package.description               | TRUE   |
| package.install_scope             | keyword | Indicating how the package was installed, e.g. user-local, global.                                                  | package.install_scope             | TRUE   |
| package.installed                 | date    | Time when package was installed.                                                                                    | package.installed                 | TRUE   |
| package.license                   | keyword | Package license.                                                                                                    | package.license                   | TRUE   |
| package.name                      | keyword | Package name.                                                                                                       | package.name                      | TRUE   |
| package.path                      | keyword | Path where the package is installed.                                                                                | package.path                      | TRUE   |
| package.reference                 | keyword | Package home page or reference URL.                                                                                 | package.reference                 | TRUE   |
| package.size                      | long    | Package size in bytes.                                                                                              | package.size                      | TRUE   |
| package.type                      | keyword | Package type.                                                                                                       | package.type                      | TRUE   |
| package.version                   | keyword | Package version.                                                                                                    | package.version                   | TRUE   |
| vulnerability.category            | keyword | Category of a vulnerability.                                                                                        | vulnerability.category            | TRUE   |
| vulnerability.classification      | keyword | Classification of the vulnerability.                                                                                | vulnerability.classification      | TRUE   |
| vulnerability.description         | keyword | Description of the vulnerability.                                                                                   | vulnerability.description         | TRUE   |
| vulnerability.description.text    | keyword | Description of the vulnerability.                                                                                   | vulnerability.description.text    | TRUE   |
| vulnerability.detected_at         | date    | Vulnerability's detection date.                                                                                     | vulnerability.detected_at         | TRUE   |
| vulnerability.enumeration         | keyword | Identifier of the vulnerability.                                                                                    | vulnerability.enumeration         | TRUE   |
| vulnerability.id                  | keyword | ID of the vulnerability.                                                                                            | vulnerability.id                  | TRUE   |
| vulnerability.published_at        | date    | Vulnerability's publication date.                                                                                   | vulnerability.published_at        | TRUE   |
| vulnerability.reference           | keyword | Reference of the vulnerability.                                                                                     | vulnerability.reference           | TRUE   |
| vulnerability.report_id           | keyword | Scan identification number.                                                                                         | vulnerability.report_id           | TRUE   |
| vulnerability.scanner.condition   | keyword | The condition matched by the package that led the scanner to consider it vulnerable.                                | vulnerability.scanner.condition   | TRUE   |
| vulnerability.scanner.reference   | keyword | Scanner's resource that provides additional information, context, and mitigations for the identified vulnerability. | vulnerability.scanner.reference   | TRUE   |
| vulnerability.scanner.source      | keyword | The origin of the decision of the scanner (AKA feed used to detect the vulnerability).                              | vulnerability.scanner.source      | TRUE   |
| vulnerability.scanner.vendor      | keyword | Name of the scanner vendor.                                                                                         | vulnerability.scanner.vendor      | TRUE   |
| vulnerability.score.base          | float   | Vulnerability Base score.                                                                                           | vulnerability.score.base          | TRUE   |
| vulnerability.score.environmental | float   | Vulnerability Environmental score.                                                                                  | vulnerability.score.environmental | TRUE   |
| vulnerability.score.temporal      | float   | Vulnerability Temporal score.                                                                                       | vulnerability.score.temporal      | TRUE   |
| vulnerability.score.version       | keyword | CVSS version.                                                                                                       | vulnerability.score.version       | TRUE   |
| vulnerability.severity            | keyword | Severity of the vulnerability.                                                                                      | vulnerability.severity            | TRUE   |
| vulnerability.under_evaluation    | boolean | Indicates if the vulnerability is awaiting analysis by the NVD.                                                     | vulnerability.under_evaluation    | TRUE   |
| wazuh.cluster.name                | keyword | Wazuh cluster name.                                                                                                 | wazuh.cluster.name                | TRUE   |
| wazuh.cluster.node                | keyword | Wazuh cluster node name.                                                                                            | wazuh.cluster.node                | TRUE   |
| wazuh.schema.version              | keyword | Wazuh schema version.                                                                                               | wazuh.schema.version              | TRUE   |
