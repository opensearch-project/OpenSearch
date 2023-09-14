# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add metrics for thread_pool task wait time ([#9681](https://github.com/opensearch-project/OpenSearch/pull/9681))

### Dependencies
- Bump OpenTelemetry from 1.26.0 to 1.30.1 ([#9950](https://github.com/opensearch-project/OpenSearch/pull/9950))
- Bump `org.apache.commons:commons-compress` from 1.23.0 to 1.24.0 ([#9973](https://github.com/opensearch-project/OpenSearch/pull/9973))
- Bump JNA version from 5.5 to 5.13 ([#9963](https://github.com/opensearch-project/OpenSearch/pull/9963))

### Changed
- Add instrumentation in rest and network layer. ([#9415](https://github.com/opensearch-project/OpenSearch/pull/9415))
- Allow parameterization of tests with OpenSearchIntegTestCase.SuiteScopeTestCase annotation ([#9916](https://github.com/opensearch-project/OpenSearch/pull/9916))

### Deprecated

### Removed

### Fixed
- Fix ignore_missing parameter has no effect when using template snippet in rename ingest processor ([#9725](https://github.com/opensearch-project/OpenSearch/pull/9725))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.10...2.x
