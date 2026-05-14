# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [repository-s3] Add support for SSE-KMS and S3 bucket owner verification ([#18312](https://github.com/opensearch-project/OpenSearch/pull/18312))

### Changed

### Dependencies
- Updated Netty to 4.1.133.Final ([#21491](https://github.com/opensearch-project/OpenSearch/pull/21491))

### Deprecated

### Removed
- [repository-s3] Removed existing ineffective `server_side_encryption` setting ([#18312](https://github.com/opensearch-project/OpenSearch/pull/18312))

### Fixed
- Fix deadlock between engineMutex and writeLock during index close and engine reset ([#11869](https://github.com/opensearch-project/OpenSearch/issues/11869))
- Harden the circuit breaker and failure handle logic in query result consumer ([#19396](https://github.com/opensearch-project/OpenSearch/pull/19396))
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix array_index_out_of_bounds_exception with wildcard and aggregations ([#20842](https://github.com/opensearch-project/OpenSearch/pull/20842))

### Security

### Changed
