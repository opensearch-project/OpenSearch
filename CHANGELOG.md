# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add API to modify a data stream's backing indices, and an `attach_to_data_stream` option on snapshot restore ([#22487](https://github.com/opensearch-project/OpenSearch/pull/22487), [#22539](https://github.com/opensearch-project/OpenSearch/pull/22539))

### Dependencies
- Updated Netty to 4.1.137.Final ([#21491](https://github.com/opensearch-project/OpenSearch/pull/21491), [#21968](https://github.com/opensearch-project/OpenSearch/pull/21968), [#22692](https://github.com/opensearch-project/OpenSearch/pull/22692))
- Update bundled JDK to 21.0.12.1+1 ([#22572](https://github.com/opensearch-project/OpenSearch/issues/22572), [#22809](https://github.com/opensearch-project/OpenSearch/issues/22809))

### Deprecated

### Removed

### Fixed
- Fix deadlock between engineMutex and writeLock during index close and engine reset ([#11869](https://github.com/opensearch-project/OpenSearch/issues/11869))
- Harden the circuit breaker and failure handle logic in query result consumer ([#19396](https://github.com/opensearch-project/OpenSearch/pull/19396))
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix array_index_out_of_bounds_exception with wildcard and aggregations ([#20842](https://github.com/opensearch-project/OpenSearch/pull/20842))
- Prevent negative fielddata stats by guarding against stale removals after shard reallocation ([#21667](https://github.com/opensearch-project/OpenSearch/pull/21667))
- Fix unbounded recursion in deserialization that can cause StackOverflowError ([#22404](https://github.com/opensearch-project/OpenSearch/pull/22404))
- Reject out-of-range WLM node threshold updates at validation time ([#22678](https://github.com/opensearch-project/OpenSearch/pull/22678))

### Security

### Changed
