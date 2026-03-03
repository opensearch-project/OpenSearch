# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add bitmap64 query support ([#20606](https://github.com/opensearch-project/OpenSearch/pull/20606))
- Add ProfilingWrapper interface for plugin access to delegates in profiling decorators ([#20607](https://github.com/opensearch-project/OpenSearch/pull/20607))
- Support expected cluster name with validation in CCS Sniff mode ([#20532](https://github.com/opensearch-project/OpenSearch/pull/20532))
- Choose the best performing node when writing with append-only index ([#20065](https://github.com/opensearch-project/OpenSearch/pull/20065))
- Add security policy to allow `accessUnixDomainSocket` in `transport-grpc` module ([#20463](https://github.com/opensearch-project/OpenSearch/pull/20463), [#20649](https://github.com/opensearch-project/OpenSearch/pull/20649))
- Add range validations in query builder and field mapper ([#20497](https://github.com/opensearch-project/OpenSearch/issues/20497))
- Support TLS cert hot-reload for Arrow Flight transport ([#20700](https://github.com/opensearch-project/OpenSearch/pull/20700))
- [Workload Management] Enhance Scroll API support for autotagging ([#20151](https://github.com/opensearch-project/OpenSearch/pull/20151))
- Add indices to search request slowlog ([#20588](https://github.com/opensearch-project/OpenSearch/pull/20588))
- Add mapper_settings support and field_mapping mapper type for pull-based ingestion([#20722](https://github.com/opensearch-project/OpenSearch/pull/20722))
- Add support for fields containing dots in their name as literals ([#19958](https://github.com/opensearch-project/OpenSearch/pull/19958))
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))
- Added public getter method in `SourceFieldMapper` to return excluded field ([#20205](https://github.com/opensearch-project/OpenSearch/pull/20205))
- Add integ test for simulating node join left event when data node cluster state publication lag because the cluster applier thread being busy ([#19907](https://github.com/opensearch-project/OpenSearch/pull/19907)).
- Relax jar hell check when extended plugins share transitive dependencies ([#20103](https://github.com/opensearch-project/OpenSearch/pull/20103))
- Added public getter method in `SourceFieldMapper` to return included field ([#20290](https://github.com/opensearch-project/OpenSearch/pull/20290))
- Support for HTTP/3 (server side) ([#20017](https://github.com/opensearch-project/OpenSearch/pull/20017))
- Add circuit breaker support for gRPC transport to prevent out-of-memory errors ([#20203](https://github.com/opensearch-project/OpenSearch/pull/20203))
- Add index-level-encryption support for snapshots and remote-store ([#20095](https://github.com/opensearch-project/OpenSearch/pull/20095))
- Adding BackWardCompatibility test for remote publication enabled cluster ([#20221](https://github.com/opensearch-project/OpenSearch/pull/20221))
- Support for hll field mapper to support cardinality rollups ([#20129](https://github.com/opensearch-project/OpenSearch/pull/20129))
- Add tracing support for StreamingRestChannel  ([#20361](https://github.com/opensearch-project/OpenSearch/pull/20361))
- Introduce new libs/netty4 module to share common implementation between netty-based plugins and modules (transport-netty4, transport-reactor-netty4) ([#20447](https://github.com/opensearch-project/OpenSearch/pull/20447))
- Add validation to make crypto store settings immutable ([#20123](https://github.com/opensearch-project/OpenSearch/pull/20123))
- Introduce concurrent translog recovery to accelerate segment replication primary promotion ([#20251](https://github.com/opensearch-project/OpenSearch/pull/20251))
- Update to `almalinux:10` ([#20482](https://github.com/opensearch-project/OpenSearch/pull/20482))
- Add X-Request-Id to uniquely identify a search request ([#19798](https://github.com/opensearch-project/OpenSearch/pull/19798))
- Added TopN selection logic for streaming terms aggregations ([#20481](https://github.com/opensearch-project/OpenSearch/pull/20481))
- Added support for Intra Segment Search ([#19704](https://github.com/opensearch-project/OpenSearch/pull/19704))
- Introduce AdditionalCodecs and EnginePlugin::getAdditionalCodecs hook to allow additional Codec registration ([#20411](https://github.com/opensearch-project/OpenSearch/pull/20411))
- Introduced strategy planner interfaces for indexing and deletion ([#20585](https://github.com/opensearch-project/OpenSearch/pull/20585))
- Implement FieldMappingIngestionMessageMapper for pull-based ingestion ([#20729](https://github.com/opensearch-project/OpenSearch/pull/20729))

### Changed
- Move Randomness from server to libs/common ([#20570](https://github.com/opensearch-project/OpenSearch/pull/20570))
- Use env variable (OPENSEARCH_FIPS_MODE) to enable opensearch to run in FIPS enforced mode instead of checking for existence of bcFIPS jars ([#20625](https://github.com/opensearch-project/OpenSearch/pull/20625))
- Update streaming flag to use search request context ([#20530](https://github.com/opensearch-project/OpenSearch/pull/20530))
- Move pull-based ingestion classes from experimental to publicAPI ([#20704](https://github.com/opensearch-project/OpenSearch/pull/20704))

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
- Leveraging segment-global ordinal mapping for efficient terms aggregation ([#20624](https://github.com/opensearch-project/OpenSearch/pull/20624))
- Support Docker distribution builds for ppc64le, arm64 and s390x ([#20678](https://github.com/opensearch-project/OpenSearch/pull/20678))
- Harden detection of HTTP/3 support by ensuring Quic native libraries are available for the target platform ([#20680](https://github.com/opensearch-project/OpenSearch/pull/20680))
- Fallback to netty client if AWS Crt client is not available on the target platform / architecture ([#20698](https://github.com/opensearch-project/OpenSearch/pull/20698))
- Fix ShardSearchFailure in transport-grpc ([#20641](https://github.com/opensearch-project/OpenSearch/pull/20641))
- Fix TLS cert hot-reload for Arrow Flight transport ([#20732](https://github.com/opensearch-project/OpenSearch/pull/20732))

### Dependencies
- Bump shadow-gradle-plugin from 8.3.9 to 9.3.1 ([#20569](https://github.com/opensearch-project/OpenSearch/pull/20569))
- Bump `ch.qos.logback:logback-core` and `ch.qos.logback:logback-classic` from 1.5.24 to 1.5.27 ([#20525](https://github.com/opensearch-project/OpenSearch/pull/20525))
- Bump `org.apache.commons:commons-text` from 1.14.0 to 1.15.0 ([#20576](https://github.com/opensearch-project/OpenSearch/pull/20576))
- Bump `aws-actions/configure-aws-credentials` from 5 to 6 ([#20577](https://github.com/opensearch-project/OpenSearch/pull/20577))
- Bump `netty` from 4.2.9.Final to 4.2.10.Final ([#20586](https://github.com/opensearch-project/OpenSearch/pull/20586))
- Bump Apache Lucene from 10.3.2 to 10.4.0 ([#20735](https://github.com/opensearch-project/OpenSearch/pull/20735))
- Bump `reactor-netty` from 1.3.2 to 1.3.3 ([#20589](https://github.com/opensearch-project/OpenSearch/pull/20589))
- Bump `reactor` from 3.8.2 to 3.8.3 ([#20589](https://github.com/opensearch-project/OpenSearch/pull/20589))
- Bump `org.jruby.jcodings:jcodings` from 1.0.63 to 1.0.64 ([#20713](https://github.com/opensearch-project/OpenSearch/pull/20713))
- Bump `org.jruby.joni:joni` from 2.2.3 to 2.2.6 ([#20714](https://github.com/opensearch-project/OpenSearch/pull/20714))
- Bump `tj-actions/changed-files` from 47.0.1 to 47.0.4 ([#20638](https://github.com/opensearch-project/OpenSearch/pull/20638), [#20716](https://github.com/opensearch-project/OpenSearch/pull/20716))
- Bump `com.nimbusds:nimbus-jose-jwt` from 10.7 to 10.8 ([#20715](https://github.com/opensearch-project/OpenSearch/pull/20715))
- Bump OpenTelemetry to 1.59.0 and OpenTelemetry Semconv to 1.40.0 ([#20737](https://github.com/opensearch-project/OpenSearch/pull/20737))

### Removed

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.6...main
