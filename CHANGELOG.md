# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added

### Dependencies
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))
- Update to Apache Lucene 9.11.0 ([#14042](https://github.com/opensearch-project/OpenSearch/pull/14042))

### Changed
- Add ability for Boolean and date field queries to run when only doc_values are enabled ([#11650](https://github.com/opensearch-project/OpenSearch/pull/11650))
- Refactor implementations of query phase searcher, allow QueryCollectorContext to have zero collectors ([#13481](https://github.com/opensearch-project/OpenSearch/pull/13481))
- Adds support to inject telemetry instances to plugins ([#13636](https://github.com/opensearch-project/OpenSearch/pull/13636))
- Updated the `indices.query.bool.max_clause_count` setting from being static to dynamically updateable ([#13568](https://github.com/opensearch-project/OpenSearch/pull/13568))

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.15...2.x
