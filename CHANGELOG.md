# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- [Rule based auto-tagging] Add get rule API ([#17336](https://github.com/opensearch-project/OpenSearch/pull/17336))
- Add multi-threaded writer support in pull-based ingestion ([#17912](https://github.com/opensearch-project/OpenSearch/pull/17912))
- Unset discovery nodes for every transport node actions request ([#17682](https://github.com/opensearch-project/OpenSearch/pull/17682))
- Implement parallel shard refresh behind cluster settings ([#17782](https://github.com/opensearch-project/OpenSearch/pull/17782))
- Bump OpenSearch Core main branch to 3.0.0 ([#18039](https://github.com/opensearch-project/OpenSearch/pull/18039))
- Update API of Message in index to add the timestamp for lag calculation in ingestion polling ([#17977](https://github.com/opensearch-project/OpenSearch/pull/17977/))


### Changed
- Change the default max header size from 8KB to 16KB. ([#18024](https://github.com/opensearch-project/OpenSearch/pull/18024))
- Avoid invalid retries in multiple replicas when querying [#17370](https://github.com/opensearch-project/OpenSearch/pull/17370)

### Dependencies
- Bump `com.google.code.gson:gson` from 2.12.1 to 2.13.0 ([#17923](https://github.com/opensearch-project/OpenSearch/pull/17923))
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.0 to 4.9.3 ([#17922](https://github.com/opensearch-project/OpenSearch/pull/17922))
- Bump `com.microsoft.azure:msal4j` from 1.18.0 to 1.20.0 ([#17925](https://github.com/opensearch-project/OpenSearch/pull/17925))

### Deprecated

### Removed

### Fixed
- Fix ingest pipeline cannot be executed when retry the failed index requests for update_by_query API and reindex API ([#18003](https://github.com/opensearch-project/OpenSearch/pull/18003))
- With creation of FilterFieldType, we need unwrap all the MappedFieldType before using the instanceof check. ([#17951](https://github.com/opensearch-project/OpenSearch/pull/17951))
- Fix simultaneously creating a snapshot and updating the repository can potentially trigger an infinite loop ([#17532](https://github.com/opensearch-project/OpenSearch/pull/17532))
- Remove package org.opensearch.transport.grpc and replace with org.opensearch.plugin.transport.grpc ([#18031](https://github.com/opensearch-project/OpenSearch/pull/18031))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/aa0e724e...main
