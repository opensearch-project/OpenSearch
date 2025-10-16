# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.3.x]
### Added

### Changed
- Faster `terms` query creation for `keyword` field with index and docValues enabled ([#19350](https://github.com/opensearch-project/OpenSearch/pull/19350))
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))
- Omit maxScoreCollector in SimpleTopDocsCollectorContext when concurrent segment search enabled ([#19584](https://github.com/opensearch-project/OpenSearch/pull/19584))
- Onboarding new maven snapshots publishing to s3 ([#19619](https://github.com/opensearch-project/OpenSearch/pull/19619))
- Remove MultiCollectorWrapper and use MultiCollector in Lucene instead ([#19595](https://github.com/opensearch-project/OpenSearch/pull/19595))

### Fixed

### Dependencies

### Deprecated

### Removed

### Security

[Unreleased 3.3.x]: https://github.com/opensearch-project/OpenSearch/compare/e972d15...3.3
