# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.3.x]
### Added

### Changed

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
- Fix NPE of ScriptScoreQuery ([#19650](https://github.com/opensearch-project/OpenSearch/pull/19650))

### Dependencies

### Deprecated

### Removed

### Security

[Unreleased 3.3.x]: https://github.com/opensearch-project/OpenSearch/compare/e972d15...3.3
