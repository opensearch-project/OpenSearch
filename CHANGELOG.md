# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Offline Nodes] Adds offline-tasks library containing various interfaces to be used for Offline Background Tasks. ([#13574](https://github.com/opensearch-project/OpenSearch/pull/13574))
- Add path prefix support to hashed prefix snapshots ([#15664](https://github.com/opensearch-project/OpenSearch/pull/15664))
- [Workload Management] QueryGroup resource cancellation framework changes ([#15651](https://github.com/opensearch-project/OpenSearch/pull/15651))

### Dependencies
- Bump `org.apache.logging.log4j:log4j-core` from 2.23.1 to 2.24.0 ([#15858](https://github.com/opensearch-project/OpenSearch/pull/15858))
- Bump `com.azure:azure-identity` from 1.13.0 to 1.13.2 ([#15578](https://github.com/opensearch-project/OpenSearch/pull/15578))
- Bump `protobuf` from 3.22.3 to 3.25.4 ([#15684](https://github.com/opensearch-project/OpenSearch/pull/15684))
- Bump `peter-evans/create-pull-request` from 6 to 7 ([#15863](https://github.com/opensearch-project/OpenSearch/pull/15863))
- Bump `com.nimbusds:oauth2-oidc-sdk` from 11.9.1 to 11.19.1 ([#15862](https://github.com/opensearch-project/OpenSearch/pull/15862))
- Bump `com.microsoft.azure:msal4j` from 1.17.0 to 1.17.1 ([#15945](https://github.com/opensearch-project/OpenSearch/pull/15945))
- Bump `ch.qos.logback:logback-core` from 1.5.6 to 1.5.8 ([#15946](https://github.com/opensearch-project/OpenSearch/pull/15946))

### Changed


### Deprecated

### Removed

### Fixed
- Fix wildcard query containing escaped character ([#15737](https://github.com/opensearch-project/OpenSearch/pull/15737))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.17...2.x
