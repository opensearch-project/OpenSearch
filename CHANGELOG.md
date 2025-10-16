# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.3.x]
### Added

### Changed

### Fixed
- Fix Allocation and Rebalance Constraints of WeightFunction are incorrectly reset ([#19012](https://github.com/opensearch-project/OpenSearch/pull/19012))
- Fix flaky test FieldDataLoadingIT.testIndicesFieldDataCacheSizeSetting ([#19571](https://github.com/opensearch-project/OpenSearch/pull/19571))
- Avoid primary shard failure caused by merged segment warmer exceptions ([#19436](https://github.com/opensearch-project/OpenSearch/pull/19436))
- Fix pull-based ingestion out-of-bounds offset scenarios and remove persisted offsets ([#19607](https://github.com/opensearch-project/OpenSearch/pull/19607))
- [Star Tree] Fix sub-aggregator casting for search with profile=true ([19652](https://github.com/opensearch-project/OpenSearch/pull/19652))
- Fix issue with updating core with a patch number other than 0 ([#19377](https://github.com/opensearch-project/OpenSearch/pull/19377))

### Dependencies

### Deprecated

### Removed

### Security

[Unreleased 3.3.x]: https://github.com/opensearch-project/OpenSearch/compare/e972d15...3.3
