# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add metrics for thread_pool task wait time ([#9681](https://github.com/opensearch-project/OpenSearch/pull/9681))
- Async blob read support for S3 plugin ([#9694](https://github.com/opensearch-project/OpenSearch/pull/9694))

### Dependencies
- Bump JNA version from 5.5 to 5.13 ([#9963](https://github.com/opensearch-project/OpenSearch/pull/9963))
- Bump `peter-evans/create-or-update-comment` from 2 to 3 ([#9575](https://github.com/opensearch-project/OpenSearch/pull/9575))
- Bump `actions/checkout` from 2 to 4 ([#9968](https://github.com/opensearch-project/OpenSearch/pull/9968))
- Bump OpenTelemetry from 1.26.0 to 1.30.1 ([#9950](https://github.com/opensearch-project/OpenSearch/pull/9950))
- Bump `org.apache.commons:commons-compress` from 1.23.0 to 1.24.0 ([#9973, #9972](https://github.com/opensearch-project/OpenSearch/pull/9973, https://github.com/opensearch-project/OpenSearch/pull/9972))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.1 to 2.23.0 ([#9971](https://github.com/opensearch-project/OpenSearch/pull/9971))
- Bump `mockito` from 5.4.0 to 5.5.0 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `bytebuddy` from 1.14.3 to 1.14.7 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `com.zaxxer:SparseBitSet` from 1.2 to 1.3 ([#10098](https://github.com/opensearch-project/OpenSearch/pull/10098))
- Bump `tibdex/github-app-token` from 1.5.0 to 2.1.0 ([#10125](https://github.com/opensearch-project/OpenSearch/pull/10125))

### Changed
- Add instrumentation in rest and network layer. ([#9415](https://github.com/opensearch-project/OpenSearch/pull/9415))
- Allow parameterization of tests with OpenSearchIntegTestCase.SuiteScopeTestCase annotation ([#9916](https://github.com/opensearch-project/OpenSearch/pull/9916))
- Add instrumentation in transport service. ([#10042](https://github.com/opensearch-project/OpenSearch/pull/10042))

### Deprecated

### Removed

### Fixed
- Fix ignore_missing parameter has no effect when using template snippet in rename ingest processor ([#9725](https://github.com/opensearch-project/OpenSearch/pull/9725))
- Fix broken backward compatibility from 2.7 for IndexSorted field indices ([#10045](https://github.com/opensearch-project/OpenSearch/pull/9725))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.11...2.x