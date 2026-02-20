# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add ProfilingWrapper interface for plugin access to delegates in profiling decorators ([#20607](https://github.com/opensearch-project/OpenSearch/pull/20607))
- Support expected cluster name with validation in CCS Sniff mode ([#20532](https://github.com/opensearch-project/OpenSearch/pull/20532))
- Choose the best performing node when writing with append-only index ([#20065](https://github.com/opensearch-project/OpenSearch/pull/20065))
- Add security policy to allow `accessUnixDomainSocket` in `transport-grpc` module ([#20463](https://github.com/opensearch-project/OpenSearch/pull/20463), [#20649](https://github.com/opensearch-project/OpenSearch/pull/20649))
- Add range validations in query builder and field mapper ([#20497](https://github.com/opensearch-project/OpenSearch/issues/20497))
- [Workload Management] Enhance Scroll API support for autotagging ([#20151](https://github.com/opensearch-project/OpenSearch/pull/20151))
- Add indices to search request slowlog ([#20588](https://github.com/opensearch-project/OpenSearch/pull/20588))

### Changed
- Move Randomness from server to libs/common ([#20570](https://github.com/opensearch-project/OpenSearch/pull/20570))
- Use env variable (OPENSEARCH_FIPS_MODE) to enable opensearch to run in FIPS enforced mode instead of checking for existence of bcFIPS jars ([#20625](https://github.com/opensearch-project/OpenSearch/pull/20625))
- Update streaming flag to use search request context ([#20530](https://github.com/opensearch-project/OpenSearch/pull/20530))

### Fixed
- Fix flaky test failures in ShardsLimitAllocationDeciderIT ([#20375](https://github.com/opensearch-project/OpenSearch/pull/20375))
- Prevent criteria update for context aware indices ([#20250](https://github.com/opensearch-project/OpenSearch/pull/20250))
- Update EncryptedBlobContainer to adhere limits while listing blobs in specific sort order if wrapped blob container supports ([#20514](https://github.com/opensearch-project/OpenSearch/pull/20514))
- [segment replication] Fix segment replication infinite retry due to stale metadata checkpoint ([#20551](https://github.com/opensearch-project/OpenSearch/pull/20551))
- Changing opensearch.cgroups.hierarchy.override causes java.lang.SecurityException exception ([#20565](https://github.com/opensearch-project/OpenSearch/pull/20565))
- Fix CriteriaBasedCodec to work with delegate codec. ([#20442](https://github.com/opensearch-project/OpenSearch/pull/20442))
- Fix WLM workload group creation failing due to updated_at clock skew ([#20486](https://github.com/opensearch-project/OpenSearch/pull/20486))
- Fix SLF4J component error ([#20587](https://github.com/opensearch-project/OpenSearch/pull/20587))
- Service does not start on Windows with OpenJDK ([#20615](https://github.com/opensearch-project/OpenSearch/pull/20615))
- Update RemoteClusterStateCleanupManager to performed batched deletions of stale ClusterMetadataManifests and address deletion timeout issues ([#20566](https://github.com/opensearch-project/OpenSearch/pull/20566))
- Fix the regression of terms agg optimization at high cardinality ([#20623](https://github.com/opensearch-project/OpenSearch/pull/20623))
- Support Docker distribution builds for ppc64le, arm64 and s390x ([#20678](https://github.com/opensearch-project/OpenSearch/pull/20678))
- Harden detection of HTTP/3 support by ensuring Quic native libraries are available for the target platform ([#20680](https://github.com/opensearch-project/OpenSearch/pull/20680))
- Fix ShardSearchFailure in transport-grpc ([#20641](https://github.com/opensearch-project/OpenSearch/pull/20641))

### Dependencies
- Bump shadow-gradle-plugin from 8.3.9 to 9.3.1 ([#20569](https://github.com/opensearch-project/OpenSearch/pull/20569))
- Bump `ch.qos.logback:logback-core` and `ch.qos.logback:logback-classic` from 1.5.24 to 1.5.27 ([#20525](https://github.com/opensearch-project/OpenSearch/pull/20525))
- Bump `org.apache.commons:commons-text` from 1.14.0 to 1.15.0 ([#20576](https://github.com/opensearch-project/OpenSearch/pull/20576))
- Bump `aws-actions/configure-aws-credentials` from 5 to 6 ([#20577](https://github.com/opensearch-project/OpenSearch/pull/20577))
- Bump `netty` from 4.2.9.Final to 4.2.10.Final ([#20586](https://github.com/opensearch-project/OpenSearch/pull/20586))
- Bump `reactor-netty` from 1.3.2 to 1.3.3 ([#20589](https://github.com/opensearch-project/OpenSearch/pull/20589))
- Bump `reactor` from 3.8.2 to 3.8.3 ([#20589](https://github.com/opensearch-project/OpenSearch/pull/20589))
- Bump `tj-actions/changed-files` from 47.0.1 to 47.0.2 ([#20638](https://github.com/opensearch-project/OpenSearch/pull/20638))

### Removed

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.6...main
