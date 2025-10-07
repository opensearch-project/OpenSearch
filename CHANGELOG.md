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
- Fix flaky test FieldDataLoadingIT.testIndicesFieldDataCacheSizeSetting ([#19571](https://github.com/opensearch-project/OpenSearch/pull/19571))

### Dependencies
- Update to Gradle 9.1 ([#19575](https://github.com/opensearch-project/OpenSearch/pull/19575))
- Bump `peter-evans/create-or-update-comment` from 4 to 5 ([#19536](https://github.com/opensearch-project/OpenSearch/pull/19536))
- Bump `com.azure:azure-core-http-netty` from 1.15.12 to 1.16.1 ([#19533](https://github.com/opensearch-project/OpenSearch/pull/19533))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.3 to 3.9.4 ([#19535](https://github.com/opensearch-project/OpenSearch/pull/19535))

### Deprecated

### Removed

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.3...main
