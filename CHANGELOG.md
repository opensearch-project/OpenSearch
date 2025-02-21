# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add execution_hint to cardinality aggregator request - backport to 2.19 (#[17420](https://github.com/opensearch-project/OpenSearch/pull/17420))

### Dependencies
- Bump netty from 4.1.117.Final to 4.1.118.Final ([#17320](https://github.com/opensearch-project/OpenSearch/pull/17320))
- Bump `jetty` version from 9.4.55.v20240627 to 9.4.57.v20241219

### Deprecated

### Removed

### Fixed
- Fix HTTP API calls that hang with 'Accept-Encoding: zstd' ([#17408](https://github.com/opensearch-project/OpenSearch/pull/17408))

### Security

[Unreleased 2.19.x]: https://github.com/opensearch-project/OpenSearch/compare/fd9a9d90df25bea1af2c6a85039692e815b894f5...2.19
