# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added

### Changed
- Disable pruning for `doc_values` for the wildcard field mapper ([#18568](https://github.com/opensearch-project/OpenSearch/pull/18568))
- Update Subject interface to use CheckedRunnable ([#18570](https://github.com/opensearch-project/OpenSearch/issues/18570))
- Update SecureAuxTransportSettingsProvider to distinguish between aux transport types ([#18616](https://github.com/opensearch-project/OpenSearch/pull/18616))
- Make node duress values cacheable ([#18649](https://github.com/opensearch-project/OpenSearch/pull/18649))
- Change default value of remote_data_ratio, which is used in Searchable Snapshots and Writeable Warm from 0 to 5 and min allowed value to 1 ([#18767](https://github.com/opensearch-project/OpenSearch/pull/18767))
- Making multi rate limiters in repository dynamic [#18069](https://github.com/opensearch-project/OpenSearch/pull/18069)
- Optimize grouping for segment concurrent search by ensuring that documents within each group are as equal as possible ([#18451](https://github.com/opensearch-project/OpenSearch/pull/18451))
- Move transport-grpc from a core plugin to a module ([#18897](https://github.com/opensearch-project/OpenSearch/pull/18897))
- Remove `experimental` designation from transport-grpc settings ([#18915](https://github.com/opensearch-project/OpenSearch/pull/18915))
- Rename package org.opensearch.plugin,transport.grpc to org.opensearch.transport.grpc ([#18923](https://github.com/opensearch-project/OpenSearch/pull/18923))

### Fixed
- Fix unnecessary refreshes on update preparation failures ([#15261](https://github.com/opensearch-project/OpenSearch/issues/15261))

### Dependencies

### Deprecated

### Removed

### Fixed
- Fix flaky tests in CloseIndexIT by addressing cluster state synchronization issues ([#18878](https://github.com/opensearch-project/OpenSearch/issues/18878))
- [Tiered Caching] Handle  query execution exception ([#19000](https://github.com/opensearch-project/OpenSearch/issues/19000))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
