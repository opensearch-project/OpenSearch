# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add multi-threaded writer support in pull-based ingestion ([#17912](https://github.com/opensearch-project/OpenSearch/pull/17912))
- Unset discovery nodes for every transport node actions request ([#17682](https://github.com/opensearch-project/OpenSearch/pull/17682))
- [Star Tree] Support of Boolean Queries in Aggregations ([#17941](https://github.com/opensearch-project/OpenSearch/pull/17941))

### Changed
- Change the default max header size from 8KB to 16KB. ([#18024](https://github.com/opensearch-project/OpenSearch/pull/18024))
- Enable concurrent_segment_search auto mode by default[#17978](https://github.com/opensearch-project/OpenSearch/pull/17978)

### Dependencies

### Deprecated

### Removed

### Fixed
- Fix ingest pipeline cannot be executed when retry the failed index requests for update_by_query API and reindex API ([#18003](https://github.com/opensearch-project/OpenSearch/pull/18003))
- With creation of FilterFieldType, we need unwrap all the MappedFieldType before using the instanceof check. ([#17951](https://github.com/opensearch-project/OpenSearch/pull/17951))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/aa0e724e...main
