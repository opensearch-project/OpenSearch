# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Adding index create block when all nodes have breached high disk watermark ([#5852](https://github.com/opensearch-project/OpenSearch/pull/5852))
- Added cluster manager throttling stats in nodes/stats API ([#5790](https://github.com/opensearch-project/OpenSearch/pull/5790))
- Added support for feature flags in opensearch.yml ([#4959](https://github.com/opensearch-project/OpenSearch/pull/4959))
- Add query for initialized extensions ([#5658](https://github.com/opensearch-project/OpenSearch/pull/5658))
- Add update-index-settings allowlist for searchable snapshot ([#5907](https://github.com/opensearch-project/OpenSearch/pull/5907))
- Replace latches with CompletableFutures for extensions ([#5646](https://github.com/opensearch-project/OpenSearch/pull/5646))
- Add support to disallow search request with preference parameter with strict weighted shard routing([#5874](https://github.com/opensearch-project/OpenSearch/pull/5874))
- Added support to apply index create block ([#4603](https://github.com/opensearch-project/OpenSearch/issues/4603))
- Adds support for minimum compatible version for extensions ([#6003](https://github.com/opensearch-project/OpenSearch/pull/6003))
- Add a guardrail to limit maximum number of shard on the cluster ([#6143](https://github.com/opensearch-project/OpenSearch/pull/6143))
- Add cancellation of in-flight SearchTasks based on resource consumption ([#5606](https://github.com/opensearch-project/OpenSearch/pull/5605))
- Add support for ppc64le architecture ([#5459](https://github.com/opensearch-project/OpenSearch/pull/5459))

### Dependencies
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))
- Bumps `reactor-netty` from 1.1.1 to 1.1.2 ([#5878](https://github.com/opensearch-project/OpenSearch/pull/5878))
- OpenJDK Update (January 2023 Patch releases) ([#6075](https://github.com/opensearch-project/OpenSearch/pull/6075))
- Bumps `Mockito` from 4.7.0 to 5.1.0, `ByteBuddy` from 1.12.18 to 1.12.22 ([#6088](https://github.com/opensearch-project/OpenSearch/pull/6088))
- Bumps `joda` from 2.10.13 to 2.12.2 ([#6095](https://github.com/opensearch-project/OpenSearch/pull/6095))
- Upgrade to Lucene 9.5.0 ([#6078](https://github.com/opensearch-project/OpenSearch/pull/6078))
- Bump antlr4 from 4.9.3 to 4.11.1 ([#6116](https://github.com/opensearch-project/OpenSearch/pull/6116))
- Bumps `Netty` from 4.1.86.Final to 4.1.87.Final ([#6130](https://github.com/opensearch-project/OpenSearch/pull/6130))
- Bumps `Jackson` from 2.14.1 to 2.14.2 ([#6129](https://github.com/opensearch-project/OpenSearch/pull/6129))

### Changed
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- [Refactor] Use local opensearch.common.SetOnce instead of lucene's utility class ([#5947](https://github.com/opensearch-project/OpenSearch/pull/5947))
- Cluster health call to throw decommissioned exception for local decommissioned node([#6008](https://github.com/opensearch-project/OpenSearch/pull/6008))
- [Refactor] core.common to new opensearch-common library ([#5976](https://github.com/opensearch-project/OpenSearch/pull/5976))
- Cluster local health call to throw exception if node is decommissioned or weighed away ([#6198](https://github.com/opensearch-project/OpenSearch/pull/6198))

### Deprecated

### Removed

### Fixed
- [Segment Replication] Fix for peer recovery ([#5344](https://github.com/opensearch-project/OpenSearch/pull/5344))
- Fix weighted shard routing state across search requests([#6004](https://github.com/opensearch-project/OpenSearch/pull/6004))
- [Segment Replication] Fix bug where inaccurate sequence numbers are sent during replication ([#6122](https://github.com/opensearch-project/OpenSearch/pull/6122))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.5...2.x
