# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added

### Dependencies
- Updated Netty to 4.1.135.Final ([#21491](https://github.com/opensearch-project/OpenSearch/pull/21491), [#21968](https://github.com/opensearch-project/OpenSearch/pull/21968))
- Updated log4j to 2.25.4 ([#21445](https://github.com/opensearch-project/OpenSearch/pull/21445))

### Deprecated

### Removed

### Fixed
- Fix deadlock between engineMutex and writeLock during index close and engine reset ([#11869](https://github.com/opensearch-project/OpenSearch/issues/11869))
- Harden the circuit breaker and failure handle logic in query result consumer ([#19396](https://github.com/opensearch-project/OpenSearch/pull/19396))
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix array_index_out_of_bounds_exception with wildcard and aggregations ([#20842](https://github.com/opensearch-project/OpenSearch/pull/20842))
- Prevent negative fielddata stats by guarding against stale removals after shard reallocation ([#21667](https://github.com/opensearch-project/OpenSearch/pull/21667))

### Security

### Changed
