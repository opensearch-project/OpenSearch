# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added

- Add support for a ForkJoinPool type ([#19008](https://github.com/opensearch-project/OpenSearch/pull/19008))
- Add seperate shard limit validation for local and remote indices ([#19532](https://github.com/opensearch-project/OpenSearch/pull/19532))
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
- Add a mapper for context aware segments grouping criteria ([#19233](https://github.com/opensearch-project/OpenSearch/pull/19233))
- Return full error for GRPC error response ([#19568](https://github.com/opensearch-project/OpenSearch/pull/19568))
- Add pluggable gRPC interceptors with explicit ordering([#19005](https://github.com/opensearch-project/OpenSearch/pull/19005))
- Add metrics for the merged segment warmer feature ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
- Add pointer based lag metric in pull-based ingestion ([#19635](https://github.com/opensearch-project/OpenSearch/pull/19635))

### Changed
- Faster `terms` query creation for `keyword` field with index and docValues enabled ([#19350](https://github.com/opensearch-project/OpenSearch/pull/19350))
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))
- Omit maxScoreCollector in SimpleTopDocsCollectorContext when concurrent segment search enabled ([#19584](https://github.com/opensearch-project/OpenSearch/pull/19584))
- Onboarding new maven snapshots publishing to s3 ([#19619](https://github.com/opensearch-project/OpenSearch/pull/19619))
- Remove MultiCollectorWrapper and use MultiCollector in Lucene instead ([#19595](https://github.com/opensearch-project/OpenSearch/pull/19595))
- Change implementation for `percentiles` aggregation for latency improvement ([#19648](https://github.com/opensearch-project/OpenSearch/pull/19648))
- Refactor the ThreadPoolStats.Stats class to use the Builder pattern instead of constructors ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))
- Refactor the IndexingStats.Stats class to use the Builder pattern instead of constructors ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))

### Fixed
- Fix Allocation and Rebalance Constraints of WeightFunction are incorrectly reset ([#19012](https://github.com/opensearch-project/OpenSearch/pull/19012))
- Fix flaky test FieldDataLoadingIT.testIndicesFieldDataCacheSizeSetting ([#19571](https://github.com/opensearch-project/OpenSearch/pull/19571))
- Avoid primary shard failure caused by merged segment warmer exceptions ([#19436](https://github.com/opensearch-project/OpenSearch/pull/19436))
- Fix pull-based ingestion out-of-bounds offset scenarios and remove persisted offsets ([#19607](https://github.com/opensearch-project/OpenSearch/pull/19607))
- Fix issue with updating core with a patch number other than 0 ([#19377](https://github.com/opensearch-project/OpenSearch/pull/19377))
- [Java Agent] Allow JRT protocol URLs in protection domain extraction ([#19683](https://github.com/opensearch-project/OpenSearch/pull/19683))

### Dependencies
- Update to Gradle 9.1 ([#19575](https://github.com/opensearch-project/OpenSearch/pull/19575))
- Bump `peter-evans/create-or-update-comment` from 4 to 5 ([#19536](https://github.com/opensearch-project/OpenSearch/pull/19536))
- Bump `com.azure:azure-core-http-netty` from 1.15.12 to 1.16.1 ([#19533](https://github.com/opensearch-project/OpenSearch/pull/19533))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.3 to 3.9.4 ([#19535](https://github.com/opensearch-project/OpenSearch/pull/19535))
- Bump `com.azure:azure-storage-common` from 12.30.2 to 12.30.3 ([#19615](https://github.com/opensearch-project/OpenSearch/pull/19615))
- Bump `peter-evans/create-issue-from-file` from 5 to 6 ([#19616](https://github.com/opensearch-project/OpenSearch/pull/19616))
- Bump `com.squareup.okhttp3:okhttp` from 5.1.0 to 5.2.1 ([#19614](https://github.com/opensearch-project/OpenSearch/pull/19614))
- Bump `com.microsoft.azure:msal4j` from 1.21.0 to 1.23.1 ([#19688](https://github.com/opensearch-project/OpenSearch/pull/19688))
- Bump `commons-net:commons-net` from 3.11.1 to 3.12.0 ([#19687](https://github.com/opensearch-project/OpenSearch/pull/19687))
- Bump `org.apache.avro:avro` from 1.12.0 to 1.12.1 ([#19692](https://github.com/opensearch-project/OpenSearch/pull/19692))
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.6 to 4.9.8 ([#19691](https://github.com/opensearch-project/OpenSearch/pull/19691))
- Bump `stefanzweifel/git-auto-commit-action` from 6 to 7 ([#19689](https://github.com/opensearch-project/OpenSearch/pull/19689))

### Deprecated
- Deprecated existing constructors in ThreadPoolStats.Stats in favor of the new Builder ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))
- Deprecated existing constructors in IndexingStats.Stats in favor of the new Builder ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))

### Removed

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.3...main
