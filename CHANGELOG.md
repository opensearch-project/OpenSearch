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
- Add support for repository with Server side encryption enabled and client side encryption as well based on a flag. ([#19630)](https://github.com/opensearch-project/OpenSearch/pull/19630))
- Add pluggable gRPC interceptors with explicit ordering([#19005](https://github.com/opensearch-project/OpenSearch/pull/19005))
- Add BindableServices extension point to transport-grpc-spi ([#19304](https://github.com/opensearch-project/OpenSearch/pull/19304))
- Add metrics for the merged segment warmer feature ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
- Add pointer based lag metric in pull-based ingestion ([#19635](https://github.com/opensearch-project/OpenSearch/pull/19635))
- Introduced internal API for retrieving metadata about requested indices from transport actions  ([#18523](https://github.com/opensearch-project/OpenSearch/pull/18523))
- Add cluster defaults for merge autoThrottle, maxMergeThreads, and maxMergeCount; Add segment size filter to the merged segment warmer ([#19629](https://github.com/opensearch-project/OpenSearch/pull/19629))
- Add build-tooling to run in FIPS environment ([#18921](https://github.com/opensearch-project/OpenSearch/pull/18921))
- Add SMILE/CBOR/YAML document format support to Bulk GRPC endpoint ([#19744](https://github.com/opensearch-project/OpenSearch/pull/19744))
- Add support for context aware segments ([#19098](https://github.com/opensearch-project/OpenSearch/pull/19098))

### Changed
- Faster `terms` query creation for `keyword` field with index and docValues enabled ([#19350](https://github.com/opensearch-project/OpenSearch/pull/19350))
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))
- Omit maxScoreCollector in SimpleTopDocsCollectorContext when concurrent segment search enabled ([#19584](https://github.com/opensearch-project/OpenSearch/pull/19584))
- Onboarding new maven snapshots publishing to s3 ([#19619](https://github.com/opensearch-project/OpenSearch/pull/19619))
- Remove MultiCollectorWrapper and use MultiCollector in Lucene instead ([#19595](https://github.com/opensearch-project/OpenSearch/pull/19595))
- Change implementation for `percentiles` aggregation for latency improvement ([#19648](https://github.com/opensearch-project/OpenSearch/pull/19648))
- Wrap checked exceptions in painless.DefBootstrap to support JDK-25 ([#19706](https://github.com/opensearch-project/OpenSearch/pull/19706))
- Refactor the ThreadPoolStats.Stats class to use the Builder pattern instead of constructors ([#19317](https://github.com/opensearch-project/OpenSearch/pull/19317))
- Refactor the IndexingStats.Stats class to use the Builder pattern instead of constructors ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))
- Remove FeatureFlag.MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG. ([#19715](https://github.com/opensearch-project/OpenSearch/pull/19715))
- Change the default value of doc_values in WildcardFieldMapper to true. ([#19796](https://github.com/opensearch-project/OpenSearch/pull/19796))
- Make Engine#loadHistoryUUID() protected and Origin#isFromTranslog() public ([#19753](https://github.com/opensearch-project/OpenSearch/pull/19752))
- Bump opensearch-protobufs dependency to 0.23.0 and update transport-grpc module compatibility ([#19831](https://github.com/opensearch-project/OpenSearch/pull/19831))

### Fixed
- Fix Allocation and Rebalance Constraints of WeightFunction are incorrectly reset ([#19012](https://github.com/opensearch-project/OpenSearch/pull/19012))
- Fix flaky test FieldDataLoadingIT.testIndicesFieldDataCacheSizeSetting ([#19571](https://github.com/opensearch-project/OpenSearch/pull/19571))
- Fix case-insensitive wildcard + aggregation query crash ([#19489](https://github.com/opensearch-project/OpenSearch/pull/19489))
- Avoid primary shard failure caused by merged segment warmer exceptions ([#19436](https://github.com/opensearch-project/OpenSearch/pull/19436))
- Fix pull-based ingestion out-of-bounds offset scenarios and remove persisted offsets ([#19607](https://github.com/opensearch-project/OpenSearch/pull/19607))
- Fix issue with updating core with a patch number other than 0 ([#19377](https://github.com/opensearch-project/OpenSearch/pull/19377))
- [Java Agent] Allow JRT protocol URLs in protection domain extraction ([#19683](https://github.com/opensearch-project/OpenSearch/pull/19683))
- Fix potential concurrent modification exception when updating allocation filters ([#19701])(https://github.com/opensearch-project/OpenSearch/pull/19701))
- Fix file-based ingestion consumer to handle start point beyond max line number([#19757])(https://github.com/opensearch-project/OpenSearch/pull/19757))
- Fix IndexOutOfBoundsException when running include/exclude on non-existent prefix in terms aggregations ([#19637](https://github.com/opensearch-project/OpenSearch/pull/19637))
- Fixed assertion unsafe use of ClusterService.state() in ResourceUsageCollectorService ([#19775])(https://github.com/opensearch-project/OpenSearch/pull/19775))
- Fix Unified highlighter for nested fields when using matchPhrasePrefixQuery ([#19442](https://github.com/opensearch-project/OpenSearch/pull/19442))
- Add S3Repository.LEGACY_MD5_CHECKSUM_CALCULATION to list of repository-s3 settings ([#19788](https://github.com/opensearch-project/OpenSearch/pull/19788))

### Dependencies
- Update to Gradle 9.2 ([#19575](https://github.com/opensearch-project/OpenSearch/pull/19575)) ([#19856](https://github.com/opensearch-project/OpenSearch/pull/19856))
- Update bundled JDK to JDK-25 ([#19314](https://github.com/opensearch-project/OpenSearch/issues/19314))
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
- Bump ch.qos.logback modules from 1.5.18 to 1.5.20 in HDFS test fixture ([#19763](https://github.com/opensearch-project/OpenSearch/pull/19763))
- Bump `github/codeql-action` from 3 to 4 ([#19785](https://github.com/opensearch-project/OpenSearch/pull/19785))
- Bump `gradle/actions` from 4 to 5 ([#19781](https://github.com/opensearch-project/OpenSearch/pull/19781))
- Bump org.bouncycastle:bc-fips from 2.1.1 to 2.1.2 ([#19818](https://github.com/opensearch-project/OpenSearch/pull/19818))
- Bump `com.google.http-client:google-http-client-gson` from 1.47.1 to 2.0.0 ([#19253](https://github.com/opensearch-project/OpenSearch/pull/19253))
- Bump `com.sun.xml.bind:jaxb-impl` from 2.2.3-1 to 4.0.6 ([#19472](https://github.com/opensearch-project/OpenSearch/pull/19472))

### Deprecated
- Deprecated existing constructors in ThreadPoolStats.Stats in favor of the new Builder ([#19317](https://github.com/opensearch-project/OpenSearch/pull/19317))
- Deprecated existing constructors in IndexingStats.Stats in favor of the new Builder ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))

### Removed

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.3...main
