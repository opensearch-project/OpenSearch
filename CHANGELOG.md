# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add server version as REST response header [#6583](https://github.com/opensearch-project/OpenSearch/issues/6583)
- Start replication checkpointTimers on primary before segments upload to remote store. ([#8221]()https://github.com/opensearch-project/OpenSearch/pull/8221)

### Dependencies
- Bump `org.apache.logging.log4j:log4j-core` from 2.17.1 to 2.20.0 ([#8307](https://github.com/opensearch-project/OpenSearch/pull/8307))
- Bump `io.grpc:grpc-context` from 1.46.0 to 1.56.1 ([#8726](https://github.com/opensearch-project/OpenSearch/pull/8726))
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.5 to 12.1.6 ([#8724](https://github.com/opensearch-project/OpenSearch/pull/8724))
- Bump `commons-codec:commons-codec` from 1.15 to 1.16.0 ([#8725](https://github.com/opensearch-project/OpenSearch/pull/8725))

### Changed
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))
- Make Span exporter configurable ([#8620](https://github.com/opensearch-project/OpenSearch/issues/8620))
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.9...2.x
