# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Adding WithFieldName interface for QueryBuilders with fieldName ([#15705](https://github.com/opensearch-project/OpenSearch/pull/15705))
- Relax the join validation for Remote State publication ([#15471](https://github.com/opensearch-project/OpenSearch/pull/15471))
- Static RemotePublication setting added, removed experimental feature flag ([#15478](https://github.com/opensearch-project/OpenSearch/pull/15478))
- MultiTermQueries in keyword fields now default to `indexed` approach and gated behind cluster setting ([#15637](https://github.com/opensearch-project/OpenSearch/pull/15637))
- [Remote Publication] Upload incremental cluster state on master re-election ([#15145](https://github.com/opensearch-project/OpenSearch/pull/15145))
- Making _cat/allocation API use indexLevelStats ([#15292](https://github.com/opensearch-project/OpenSearch/pull/15292))
- Memory optimisations in _cluster/health API ([#15492](https://github.com/opensearch-project/OpenSearch/pull/15492))

### Dependencies
- Bump `com.azure:azure-identity` from 1.13.0 to 1.13.2 ([#15578](https://github.com/opensearch-project/OpenSearch/pull/15578))
- Bump `protobuf` from 3.22.3 to 3.25.4 ([#15684](https://github.com/opensearch-project/OpenSearch/pull/15684))
- Bump `org.apache.logging.log4j:log4j-core` from 2.23.1 to 2.24.0 ([#15858](https://github.com/opensearch-project/OpenSearch/pull/15858))
- Bump `peter-evans/create-pull-request` from 6 to 7 ([#15863](https://github.com/opensearch-project/OpenSearch/pull/15863))
- Bump `com.nimbusds:oauth2-oidc-sdk` from 11.9.1 to 11.19.1 ([#15862](https://github.com/opensearch-project/OpenSearch/pull/15862))
- Bump `com.google.cloud:google-cloud-storage` from 1.113.1 to 2.42.0 ([#15421](https://github.com/opensearch-project/OpenSearch/pull/15421))

### Changed


### Deprecated

### Removed

### Fixed

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.17...2.x
