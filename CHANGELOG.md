# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add multi-threaded writer support in pull-based ingestion ([#17912](https://github.com/opensearch-project/OpenSearch/pull/17912))
- Unset discovery nodes for every transport node actions request ([#17682](https://github.com/opensearch-project/OpenSearch/pull/17682))

### Changed

### Dependencies

### Changed

### Deprecated

### Removed

### Fixed
- With creation of FilterFieldType, we need unwrap all the MappedFieldType before using the instanceof check. ([#17951](https://github.com/opensearch-project/OpenSearch/pull/17951))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/aa0e724e...main
