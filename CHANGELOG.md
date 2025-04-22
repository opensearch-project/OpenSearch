# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add multi-threaded writer support in pull-based ingestion ([#17912](https://github.com/opensearch-project/OpenSearch/pull/17912))
- Unset discovery nodes for every transport node actions request ([#17682](https://github.com/opensearch-project/OpenSearch/pull/17682))
- Implement parallel shard refresh behind cluster settings ([#17782](https://github.com/opensearch-project/OpenSearch/pull/17782))

### Changed
- Change the default max header size from 8KB to 16KB. ([#18024](https://github.com/opensearch-project/OpenSearch/pull/18024))

### Dependencies
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.0 to 4.9.3 ([#17922](https://github.com/opensearch-project/OpenSearch/pull/17922))

### Dependencies

### Deprecated

### Removed

### Fixed
- With creation of FilterFieldType, we need unwrap all the MappedFieldType before using the instanceof check. ([#17951](https://github.com/opensearch-project/OpenSearch/pull/17951))
- Fix simultaneously creating a snapshot and updating the repository can potentially trigger an infinite loop ([#17532](https://github.com/opensearch-project/OpenSearch/pull/17532))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/aa0e724e...main
