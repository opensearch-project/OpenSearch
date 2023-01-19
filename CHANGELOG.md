
# CHANGELOG
## [Unreleased]
### Dependencies
- Bumps `reactor-netty` from 1.1.1 to 1.1.2 ([#5878](https://github.com/opensearch-project/OpenSearch/pull/5878))

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
- Add query for initialized extensions ([#5658](https://github.com/opensearch-project/OpenSearch/pull/5658))

### Added
- Added cluster manager throttling stats in nodes/stats API ([#5790](https://github.com/opensearch-project/OpenSearch/pull/5790))
- Add support of default replica count cluster setting ([#5610](https://github.com/opensearch-project/OpenSearch/pull/5610))
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5069](https://github.com/opensearch-project/OpenSearch/pull/5069))
- Update to Gradle 7.6 ([#5382](https://github.com/opensearch-project/OpenSearch/pull/5382))
- Reject bulk requests with invalid actions ([#5299](https://github.com/opensearch-project/OpenSearch/issues/5299))
- Add max_shard_size parameter for shrink API ([#5229](https://github.com/opensearch-project/OpenSearch/pull/5229))
- Added jackson dependency to server ([#5366](https://github.com/opensearch-project/OpenSearch/pull/5366))
- Adding support to register settings dynamically ([#5495](https://github.com/opensearch-project/OpenSearch/pull/5495))
- Adding auto release workflow ([#5582](https://github.com/opensearch-project/OpenSearch/pull/5582))
- Added experimental support for extensions ([#5347](https://github.com/opensearch-project/OpenSearch/pull/5347)), ([#5518](https://github.com/opensearch-project/OpenSearch/pull/5518)), ([#5597](https://github.com/opensearch-project/OpenSearch/pull/5597)), ([#5615](https://github.com/opensearch-project/OpenSearch/pull/5615)))
- Add CI bundle pattern to distribution download ([#5348](https://github.com/opensearch-project/OpenSearch/pull/5348))
- Added @gbbafna as an OpenSearch maintainer ([#5668](https://github.com/opensearch-project/OpenSearch/pull/5668))
- Experimental support for extended backward compatiblity in searchable snapshots ([#5429](https://github.com/opensearch-project/OpenSearch/pull/5429))
- Add feature flag for extensions ([#5211](https://github.com/opensearch-project/OpenSearch/pull/5211))
- Add support for s390x architecture ([#4001](https://github.com/opensearch-project/OpenSearch/pull/4001))
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Point in time rest layer changes for create and delete PIT API ([#4064](https://github.com/opensearch-project/OpenSearch/pull/4064))
- Add failover support with Segment Replication enabled. ([#4325](https://github.com/opensearch-project/OpenSearch/pull/4325)
- Point in time rest layer changes for list PIT and PIT segments API ([#4388](https://github.com/opensearch-project/OpenSearch/pull/4388))
- Added @dreamer-89 as an Opensearch maintainer ([#4342](https://github.com/opensearch-project/OpenSearch/pull/4342))
- Added release notes for 1.3.5 ([#4343](https://github.com/opensearch-project/OpenSearch/pull/4343))
- Added release notes for 2.2.1 ([#4344](https://github.com/opensearch-project/OpenSearch/pull/4344))
- BWC version 2.2.2 ([#4383](https://github.com/opensearch-project/OpenSearch/pull/4383))
- Support for labels on version bump PRs, skip label support for changelog verifier ([#4391](https://github.com/opensearch-project/OpenSearch/pull/4391))
- Add a new node role 'search' which is dedicated to provide search capability ([#4689](https://github.com/opensearch-project/OpenSearch/pull/4689))
- Introduce experimental searchable snapshot API ([#4680](https://github.com/opensearch-project/OpenSearch/pull/4680))
- Added support for feature flags in opensearch.yml ([#4959](https://github.com/opensearch-project/OpenSearch/pull/4959))

### Dependencies
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))
### Changed
### Deprecated
### Removed
### Fixed
### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.5...2.x
