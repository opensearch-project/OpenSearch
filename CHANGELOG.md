# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
- Add a mapper for context aware segments grouping criteria ([#19233](https://github.com/opensearch-project/OpenSearch/pull/19233))

### Changed
- Faster `terms` query creation for `keyword` field with index and docValues enabled ([#19350](https://github.com/opensearch-project/OpenSearch/pull/19350))
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))

### Fixed
- Fix unnecessary refreshes on update preparation failures ([#15261](https://github.com/opensearch-project/OpenSearch/issues/15261))
- Fix NullPointerException in segment replicator ([#18997](https://github.com/opensearch-project/OpenSearch/pull/18997))
- Ensure that plugins that utilize dumpCoverage can write to jacoco.dir when tests.security.manager is enabled ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))
- Fix OOM due to large number of shard result buffering ([#19066](https://github.com/opensearch-project/OpenSearch/pull/19066))
- Fix flaky tests in CloseIndexIT by addressing cluster state synchronization issues ([#18878](https://github.com/opensearch-project/OpenSearch/issues/18878))
- [Tiered Caching] Handle  query execution exception ([#19000](https://github.com/opensearch-project/OpenSearch/issues/19000))
- Grant access to testclusters dir for tests ([#19085](https://github.com/opensearch-project/OpenSearch/issues/19085))
- Fix assertion error when collapsing search results with concurrent segment search enabled ([#19053](https://github.com/opensearch-project/OpenSearch/pull/19053))
- Fix skip_unavailable setting changing to default during node drop issue ([#18766](https://github.com/opensearch-project/OpenSearch/pull/18766))
- Fix issue with s3-compatible repositories due to missing checksum trailing headers ([#19220](https://github.com/opensearch-project/OpenSearch/pull/19220))
- Add reference count control in NRTReplicationEngine#acquireLastIndexCommit ([#19214](https://github.com/opensearch-project/OpenSearch/pull/19214))
- Fix pull-based ingestion pause state initialization during replica promotion ([#19212](https://github.com/opensearch-project/OpenSearch/pull/19212))
- Fix QueryPhaseResultConsumer incomplete callback loops ([#19231](https://github.com/opensearch-project/OpenSearch/pull/19231))
- Fix the `scaled_float` precision issue ([#19188](https://github.com/opensearch-project/OpenSearch/pull/19188))
- Fix Using an excessively large reindex slice can lead to a JVM OutOfMemoryError on coordinator.([#18964](https://github.com/opensearch-project/OpenSearch/pull/18964))
- [Flaky Test] Fix flaky test in SecureReactorNetty4HttpServerTransportTests with reproducible seed ([#19327](https://github.com/opensearch-project/OpenSearch/pull/19327))
- Remove unnecessary looping in field data cache clear ([#19116](https://github.com/opensearch-project/OpenSearch/pull/19116))
- [Flaky Test] Fix flaky test IngestFromKinesisIT.testAllActiveIngestion ([#19380](https://github.com/opensearch-project/OpenSearch/pull/19380))
- Fix lag metric for pull-based ingestion when streaming source is empty ([#19393](https://github.com/opensearch-project/OpenSearch/pull/19393))
- Fix ingestion state xcontent serialization in IndexMetadata and fail fast on mapping errors([#19320](https://github.com/opensearch-project/OpenSearch/pull/19320))
- Fix updated keyword field params leading to stale responses from request cache ([#19385](https://github.com/opensearch-project/OpenSearch/pull/19385))
- Fix Allocation and Rebalance Constraints of WeightFunction are incorrectly reset ([#19012](https://github.com/opensearch-project/OpenSearch/pull/19012))
- Fix flaky test FieldDataLoadingIT.testIndicesFieldDataCacheSizeSetting ([#19571](https://github.com/opensearch-project/OpenSearch/pull/19571))

### Dependencies
- Bump `peter-evans/create-or-update-comment` from 4 to 5 ([#19536](https://github.com/opensearch-project/OpenSearch/pull/19536))
- Bump `com.azure:azure-core-http-netty` from 1.15.12 to 1.16.1 ([#19533](https://github.com/opensearch-project/OpenSearch/pull/19533))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.3 to 3.9.4 ([#19535](https://github.com/opensearch-project/OpenSearch/pull/19535))

### Deprecated

### Removed

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.3...main
