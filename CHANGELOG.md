# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for Warm Indices Write Block on Flood Watermark breach ([#18375](https://github.com/opensearch-project/OpenSearch/pull/18375))
- Ability to run Code Coverage with Gradle and produce the jacoco reports locally ([#18509](https://github.com/opensearch-project/OpenSearch/issues/18509))
- Add NodeResourceUsageStats to ClusterInfo ([#18480](https://github.com/opensearch-project/OpenSearch/issues/18472))
- Introduce SecureHttpTransportParameters experimental API (to complement SecureTransportParameters counterpart) ([#18572](https://github.com/opensearch-project/OpenSearch/issues/18572))
- Create equivalents of JSM's AccessController in the java agent ([#18346](https://github.com/opensearch-project/OpenSearch/issues/18346))
- Introduced a new cluster-level API to fetch remote store metadata (segments and translogs) for each shard of an index. ([#18257](https://github.com/opensearch-project/OpenSearch/pull/18257))
- Add support for search pipeline in msearch template ([#18564](https://github.com/opensearch-project/OpenSearch/pull/18564))
- Add support for search pipeline in search and msearch template ([#18564](https://github.com/opensearch-project/OpenSearch/pull/18564))

### Changed
- Update Subject interface to use CheckedRunnable ([#18570](https://github.com/opensearch-project/OpenSearch/issues/18570))

### Dependencies
- Bump `stefanzweifel/git-auto-commit-action` from 5 to 6 ([#18524](https://github.com/opensearch-project/OpenSearch/pull/18524))
- Bump Apache Lucene to 10.2.2 ([#18573](https://github.com/opensearch-project/OpenSearch/pull/18573))
- Bump `org.apache.logging.log4j:log4j-core` from 2.24.3 to 2.25.0 ([#18589](https://github.com/opensearch-project/OpenSearch/pull/18589))
- Bump `com.google.code.gson:gson` from 2.13.0 to 2.13.1 ([#18585](https://github.com/opensearch-project/OpenSearch/pull/18585))
- Bump `com.azure:azure-core-http-netty` from 1.15.11 to 1.15.12 ([#18586](https://github.com/opensearch-project/OpenSearch/pull/18586))

### Deprecated

### Removed

### Fixed
- Add task cancellation checks in aggregators ([#18426](https://github.com/opensearch-project/OpenSearch/pull/18426))
- Fix concurrent timings in profiler ([#18540](https://github.com/opensearch-project/OpenSearch/pull/18540))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
