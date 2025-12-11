# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))

### Changed
- Handle custom metadata files in subdirectory-store ([#20157](https://github.com/opensearch-project/OpenSearch/pull/20157))
- Add support for missing proto fields in GRPC FunctionScore and Highlight ([#20169](https://github.com/opensearch-project/OpenSearch/pull/20169))

### Fixed
- Fix flaky test ClusterMaxMergesAtOnceIT.testClusterLevelDefaultUpdatesMergePolicy ([#18056](https://github.com/opensearch-project/OpenSearch/issues/18056))
- Fix issue with updating core with a patch number other than 0 ([#19377](https://github.com/opensearch-project/OpenSearch/pull/19377))
- [Java Agent] Allow JRT protocol URLs in protection domain extraction ([#19683](https://github.com/opensearch-project/OpenSearch/pull/19683))
- Fix potential concurrent modification exception when updating allocation filters ([#19701])(https://github.com/opensearch-project/OpenSearch/pull/19701))
- Fix wildcard query with escaped backslash followed by wildcard character ([#19719](https://github.com/opensearch-project/OpenSearch/pull/19719))
- Fix file-based ingestion consumer to handle start point beyond max line number([#19757])(https://github.com/opensearch-project/OpenSearch/pull/19757))
- Fix IndexOutOfBoundsException when running include/exclude on non-existent prefix in terms aggregations ([#19637](https://github.com/opensearch-project/OpenSearch/pull/19637))
- Fixed assertion unsafe use of ClusterService.state() in ResourceUsageCollectorService ([#19775])(https://github.com/opensearch-project/OpenSearch/pull/19775))
- Fix Unified highlighter for nested fields when using matchPhrasePrefixQuery ([#19442](https://github.com/opensearch-project/OpenSearch/pull/19442))
- Add S3Repository.LEGACY_MD5_CHECKSUM_CALCULATION to list of repository-s3 settings ([#19788](https://github.com/opensearch-project/OpenSearch/pull/19788))
- Fix NullPointerException when restoring remote snapshot with missing shard size information ([#19684](https://github.com/opensearch-project/OpenSearch/pull/19684))
- Fix NPE of ScriptScoreQuery ([#19650](https://github.com/opensearch-project/OpenSearch/pull/19650))
- Fix ClassCastException in FlightClientChannel for requests larger than 16KB ([#20010](https://github.com/opensearch-project/OpenSearch/pull/20010))
- Fix GRPC Bulk ([#19937](https://github.com/opensearch-project/OpenSearch/pull/19937))
- Fix node bootstrap error when enable stream transport and remote cluster state ([#19948](https://github.com/opensearch-project/OpenSearch/pull/19948))
- Keep track and release Reactor Netty 4 Transport accepted Http Channels during the Node shutdown ([#20106](https://github.com/opensearch-project/OpenSearch/pull/20106))
- Fix deletion failure/error of unused index template; case when an index template matches a data stream but has a lower priority. ([#20102](https://github.com/opensearch-project/OpenSearch/pull/20102))
- Fixed version incompatibility in remote state entities using bytestream for ser/de ([#20080](https://github.com/opensearch-project/OpenSearch/pull/20080))
- Fix toBuilder method in EngineConfig to include mergedSegmentTransferTracker([#20105](https://github.com/opensearch-project/OpenSearch/pull/20105))
- Fixed handling of property index in BulkRequest during deserialization ([#20132](https://github.com/opensearch-project/OpenSearch/pull/20132))
- Fix negative CPU usage values in node stats ([#19120](https://github.com/opensearch-project/OpenSearch/issues/19120))
- Fix duplicate registration of FieldDataCache dynamic setting ([20140](https://github.com/opensearch-project/OpenSearch/pull/20140))
- Fix array out of bounds during aggregation ([#20204](https://github.com/opensearch-project/OpenSearch/pull/20204))
- Fix bug in Assertion framework(Yaml Rest test): numeric comparison fails when comparing Integer vs Long (or Float vs Double) ([#19376](https://github.com/opensearch-project/OpenSearch/pull/19376))

### Dependencies
- Bump `actions/checkout` from 5 to 6 ([#20186](https://github.com/opensearch-project/OpenSearch/pull/20186))
- Bump `org.apache.commons:commons-configuration2` from 2.12.0 to 2.13.0 ([#20185](https://github.com/opensearch-project/OpenSearch/pull/20185))

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.4...main
