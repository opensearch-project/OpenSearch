# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added

### Changed

### Fixed
- Fix unnecessary refreshes on update preparation failures ([#15261](https://github.com/opensearch-project/OpenSearch/issues/15261))

### Dependencies
- Bump `commons-cli:commons-cli` from 1.9.0 to 1.10.0 ([#19021](https://github.com/opensearch-project/OpenSearch/pull/19021))

### Deprecated

### Removed

### Fixed
- Fix flaky tests in CloseIndexIT by addressing cluster state synchronization issues ([#18878](https://github.com/opensearch-project/OpenSearch/issues/18878))
- [Tiered Caching] Handle  query execution exception ([#19000](https://github.com/opensearch-project/OpenSearch/issues/19000))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
