# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 4.13.x]
### Added

### Changed
- Handle custom metadata files in subdirectory-store ([#20157](https://github.com/opensearch-project/OpenSearch/pull/20157))
- Add support for missing proto fields in GRPC FunctionScore and Highlight ([#20169](https://github.com/opensearch-project/OpenSearch/pull/20169))
- Ensure all modules are included in INTEG_TEST testcluster distribution ([#20241](https://github.com/opensearch-project/OpenSearch/pull/20241))
- Cleanup HttpServerTransport.Dispatcher in Netty tests ([#20160](https://github.com/opensearch-project/OpenSearch/pull/20160))
- Use compact object headers with JDK25+ ([#20392](https://github.com/opensearch-project/OpenSearch/pull/20392))
- Add `cluster.initial_cluster_manager_nodes` to testClusters OVERRIDABLE_SETTINGS ([#20348](https://github.com/opensearch-project/OpenSearch/pull/20348))
- Add BigInteger support for unsigned_long fields in gRPC transport ([#20346](https://github.com/opensearch-project/OpenSearch/pull/20346))
- Install demo security information when running ./gradlew run -PinstalledPlugins="['opensearch-security']" ([#20372](https://github.com/opensearch-project/OpenSearch/pull/20372))
- Add `Alt-Svc` header support to advertise HTTP/3 availability ([#20434](https://github.com/opensearch-project/OpenSearch/pull/20434))
- Refactor streaming agg query phase planning ([#20471](https://github.com/opensearch-project/OpenSearch/pull/20471))

### Fixed
- Fix Snapshot rename replacement unbounded length rename ([#20464](https://github.com/opensearch-project/OpenSearch/issues/20464))
- Fix segment replication failure during rolling restart ([#19234](https://github.com/opensearch-project/OpenSearch/issues/19234))
- Fix bug of warm index: FullFileCachedIndexInput was closed error ([#20055](https://github.com/opensearch-project/OpenSearch/pull/20055))
- Fix flaky test ClusterMaxMergesAtOnceIT.testClusterLevelDefaultUpdatesMergePolicy ([#18056](https://github.com/opensearch-project/OpenSearch/issues/18056))
- Fix bug in Assertion framework(Yaml Rest test): numeric comparison fails when comparing Integer vs Long (or Float vs Double) ([#19376](https://github.com/opensearch-project/OpenSearch/pull/19376))
- Fix Netty deprecation warnings in transport-netty4 module ([#20233](https://github.com/opensearch-project/OpenSearch/pull/20233))
- Fix snapshot restore when an index sort is present ([#20284](https://github.com/opensearch-project/OpenSearch/pull/20284))
- Fix SearchPhaseExecutionException to properly initCause ([#20320](https://github.com/opensearch-project/OpenSearch/pull/20320))
- [repository-s3] remove endpointOverride and let AWS SDK V2 S3 determine the s3 url based on bucket name or arn provided ([#20345](https://github.com/opensearch-project/OpenSearch/pull/20345))
- Fix `cluster.remote.<cluster_alias>.server_name` setting no populating SNI ([#20321](https://github.com/opensearch-project/OpenSearch/pull/20321))
- Fix X-Opaque-Id header propagation (along with other response headers) for streaming Reactor Netty 4 transport ([#20371](https://github.com/opensearch-project/OpenSearch/pull/20371))
- Allow removing plugin that's optionally extended ([#20417](https://github.com/opensearch-project/OpenSearch/pull/20417))
- Fix indexing regression and bug fixes for grouping criteria. ([20145](https://github.com/opensearch-project/OpenSearch/pull/20145))
- LeafReader should not remove SubReaderWrappers incase IndexWriter encounters a non aborting Exception ([#20193](https://github.com/opensearch-project/OpenSearch/pull/20193))
- Fix Netty deprecation warnings in transport-reactor-netty4 module ([20429](https://github.com/opensearch-project/OpenSearch/pull/20429))
- Fix stats aggregation returning zero results with `size:0`. ([20427](https://github.com/opensearch-project/OpenSearch/pull/20427))
- Fix the node local term and version being truncated in logs when host providers return very long IP or host strings ([20432](https://github.com/opensearch-project/OpenSearch/pull/20432))
- Remove child level directory on refresh for CompositeIndexWriter ([#20326](https://github.com/opensearch-project/OpenSearch/pull/20326))
- Fixes and refactoring in stream transport to make it more robust ([#20359](https://github.com/opensearch-project/OpenSearch/pull/20359))

### Dependencies

### Changed

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 4.12.x]: https://github.com/wazuh/wazuh-indexer/compare/4.12.0...4.13.0
