# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added

### Dependencies
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))
- Update to Apache Lucene 9.11.0 ([#14042](https://github.com/opensearch-project/OpenSearch/pull/14042))

### Changed

### Deprecated

### Removed

### Fixed
- Fix get field mapping API returns 404 error in mixed cluster with multiple versions ([#13624](https://github.com/opensearch-project/OpenSearch/pull/13624))
- Allow clearing `remote_store.compatibility_mode` setting ([#13646](https://github.com/opensearch-project/OpenSearch/pull/13646))
- Fix ReplicaShardBatchAllocator to batch shards without duplicates ([#13710](https://github.com/opensearch-project/OpenSearch/pull/13710))
- Don't return negative scores from `multi_match` query with `cross_fields` type  ([#13829](https://github.com/opensearch-project/OpenSearch/pull/13829))
- Painless: ensure type "UnmodifiableMap" for params ([#13885](https://github.com/opensearch-project/OpenSearch/pull/13885))
- Pass parent filter to inner hit query ([#13903](https://github.com/opensearch-project/OpenSearch/pull/13903))
- Fix NPE on restore searchable snapshot ([#13911](https://github.com/opensearch-project/OpenSearch/pull/13911))
- Fix double invocation of postCollection when MultiBucketCollector is present ([#14015](https://github.com/opensearch-project/OpenSearch/pull/14015))
- Java high-level REST client bulk() is not respecting the bulkRequest.requireAlias(true) method call ([#14146](https://github.com/opensearch-project/OpenSearch/pull/14146))
- Fix ShardNotFoundException during request cache clean up ([#14219](https://github.com/opensearch-project/OpenSearch/pull/14219))
- Refactoring FilterPath.parse by using an iterative approach ([#14200](https://github.com/opensearch-project/OpenSearch/pull/14200))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.15...2.x
