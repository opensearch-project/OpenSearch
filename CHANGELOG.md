# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 5.0.0]
### Added
- Add new users, roles and mappings [(#886)](https://github.com/wazuh/wazuh-indexer/pull/886)
- Add custom GitHub Action to validate commiter's emails by domain [(#896)](https://github.com/wazuh/wazuh-indexer/pull/896)
- Migrate to OpenSearch 3.0.0 [(#903)](https://github.com/wazuh/wazuh-indexer/pull/903)
- Add Wazuh version comparison [(#936)](https://github.com/wazuh/wazuh-indexer/pull/936)

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
-

### Changed
- Migrate issue templates to 5.0.0 [(#855)](https://github.com/wazuh/wazuh-indexer/pull/855)
- Migrate workflows and scripts from 6.0.0 [(861)](https://github.com/wazuh/wazuh-indexer/pull/861)
- Migrate smoke tests to 5.0.0 [(#863)](https://github.com/wazuh/wazuh-indexer/pull/863)
- Replace and remove deprecated settings [(#894)](https://github.com/wazuh/wazuh-indexer/pull/894)
- Backport packaging improvements [(#906)](https://github.com/wazuh/wazuh-indexer/pull/906)
- Apply Lintian overrides [(#908)](https://github.com/wazuh/wazuh-indexer/pull/908)
- Add noninteractive option for DEB packages testing [(#914)](https://github.com/wazuh/wazuh-indexer/pull/914)
- Migrate builder workflows from [(#930)](https://github.com/wazuh/wazuh-indexer/pull/930)

### Deprecated
-

### Removed
- Remove extra files [(#866)](https://github.com/wazuh/wazuh-indexer/pull/866)
- Remove references to legacy VERSION file [(#908)](https://github.com/wazuh/wazuh-indexer/pull/908)
- Remove opensearch-performance-analyzer [(#892)](https://github.com/wazuh/wazuh-indexer/pull/892)

### Fixed
- Fix package upload to bucket subfolder 5.x [(#846)](https://github.com/wazuh/wazuh-indexer/pull/846)
- Fix seccomp error on `wazuh-indexer.service` [(#912)](https://github.com/wazuh/wazuh-indexer/pull/912)

### Security
- Reduce risk of GITHUB_TOKEN exposure [(#960)](https://github.com/wazuh/wazuh-indexer/pull/960)

[Unreleased 5.0.0]: https://github.com/wazuh/wazuh-indexer/compare/4.14.0...5.0.0
