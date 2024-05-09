# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add useCompoundFile index setting ([#13478](https://github.com/opensearch-project/OpenSearch/pull/13478))
- [Offline Nodes] Adds offline-tasks library containing various interfaces to be used for Offline Background Tasks. ([#13575](https://github.com/opensearch-project/OpenSearch/issues/13575))

### Dependencies
- Bump `com.github.spullara.mustache.java:compiler` from 0.9.10 to 0.9.13 ([#13329](https://github.com/opensearch-project/OpenSearch/pull/13329), [#13559](https://github.com/opensearch-project/OpenSearch/pull/13559))
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))
- Bump `org.apache.commons:commons-text` from 1.11.0 to 1.12.0 ([#13557](https://github.com/opensearch-project/OpenSearch/pull/13557))
- Bump `org.hdrhistogram:HdrHistogram` from 2.1.12 to 2.2.1 ([#13556](https://github.com/opensearch-project/OpenSearch/pull/13556))

### Changed
- Add ability for Boolean and date field queries to run when only doc_values are enabled ([#11650](https://github.com/opensearch-project/OpenSearch/pull/11650))
- Refactor implementations of query phase searcher, allow QueryCollectorContext to have zero collectors ([#13481](https://github.com/opensearch-project/OpenSearch/pull/13481))

### Deprecated

### Removed
- Remove handling of index.mapper.dynamic in AutoCreateIndex([#13067](https://github.com/opensearch-project/OpenSearch/pull/13067))

### Fixed
- Fix negative RequestStats metric issue ([#13553](https://github.com/opensearch-project/OpenSearch/pull/13553))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
