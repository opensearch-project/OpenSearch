# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added

### Dependencies
- Bump `log4j` from 2.21.0 to 2.25.3 ([#20308](https://github.com/opensearch-project/OpenSearch/pull/20308))
- Bump netty from 4.1.125.Final to 4.1.131.Final ([#20744](https://github.com/opensearch-project/OpenSearch/pull/20744))
- Bump shadow-gradle-plugin from 8.1.1 to 8.3.10 ([#20569](https://github.com/opensearch-project/OpenSearch/pull/20569))
- Bump Jackson from 2.18.2 to 2.18.6 ([#20813](https://github.com/opensearch-project/OpenSearch/pull/20813))
- Update HDFS test fixture dependencies ([#20768](https://github.com/opensearch-project/OpenSearch/pull/20768))

### Deprecated

### Removed

### Fixed
- Fix segment replication failure during rolling restart ([#19234](https://github.com/opensearch-project/OpenSearch/issues/19234))
- Harden the circuit breaker and failure handle logic in query result consumer ([#19396](https://github.com/opensearch-project/OpenSearch/pull/19396))
- Fix SearchPhaseExecutionException to properly initCause ([#20336](https://github.com/opensearch-project/OpenSearch/pull/20336))

### Security

### Changed
- Make XContentMapValues.filter case insensitive ([#19976](https://github.com/opensearch-project/OpenSearch/pull/19976))
