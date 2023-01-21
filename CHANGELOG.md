# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Added cluster manager throttling stats in nodes/stats API ([#5790](https://github.com/opensearch-project/OpenSearch/pull/5790))
- Added support for feature flags in opensearch.yml ([#4959](https://github.com/opensearch-project/OpenSearch/pull/4959))
- Add query for initialized extensions ([#5658](https://github.com/opensearch-project/OpenSearch/pull/5658))
- Add update-index-settings allowlist for searchable snapshot ([#5907](https://github.com/opensearch-project/OpenSearch/pull/5907))

### Dependencies
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))
- Bumps `reactor-netty` from 1.1.1 to 1.1.2 ([#5878](https://github.com/opensearch-project/OpenSearch/pull/5878))

### Changed
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- [Refactor] Use local opensearch.common.SetOnce instead of lucene's utility class ([#5947](https://github.com/opensearch-project/OpenSearch/pull/5947))

### Deprecated

### Removed

### Fixed
- [Segment Replication] Fix for peer recovery ([#5344](https://github.com/opensearch-project/OpenSearch/pull/5344))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.5...2.x
