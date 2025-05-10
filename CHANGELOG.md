# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0.x]
### Added
Improve sort-query performance by retaining the default `totalHitsThreshold` for approximated `match_all` queries ([#18189](https://github.com/opensearch-project/OpenSearch/pull/18189))

### Changed

### Dependencies
- Update Apache HttpClient5 and HttpCore5 (CVE-2025-27820) ([#18152](https://github.com/opensearch-project/OpenSearch/pull/18152))
- Bump `netty` from 4.1.118.Final to 4.1.121.Final ([#18192](https://github.com/opensearch-project/OpenSearch/pull/18192))
- Bump `reactor-netty` from 1.2.4 to 1.2.5 ([#18243](https://github.com/opensearch-project/OpenSearch/pull/18243))
- Bump `reactor` from 3.5.20 to 3.7.5 ([#18243](https://github.com/opensearch-project/OpenSearch/pull/18243))

### Deprecated

### Removed

### Fixed
- Fix the native plugin installation error cause by the pgp public key change ([#18147](https://github.com/opensearch-project/OpenSearch/pull/18147))

### Security

[Unreleased 3.0.x]: https://github.com/opensearch-project/OpenSearch/compare/bb0dff04...3.0
