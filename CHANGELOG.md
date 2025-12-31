# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))
- Added public getter method in `SourceFieldMapper` to return excluded field ([#20205](https://github.com/opensearch-project/OpenSearch/pull/20205))
- Add integ test for simulating node join left event when data node cluster state publication lag because the cluster applier thread being busy ([#19907](https://github.com/opensearch-project/OpenSearch/pull/19907)).
- Relax jar hell check when extended plugins share transitive dependencies ([#20103](https://github.com/opensearch-project/OpenSearch/pull/20103))
- Added public getter method in `SourceFieldMapper` to return included field ([#20290](https://github.com/opensearch-project/OpenSearch/pull/20290))
- Choose the best performing node when writing with append-only index ([#20065](https://github.com/opensearch-project/OpenSearch/pull/20065))

### Changed

### Fixed

### Dependencies

### Removed

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.5...main
