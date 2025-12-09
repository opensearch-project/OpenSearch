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
- Add circuit breaker support for gRPC transport to prevent out-of-memory errors ([#20203](https://github.com/opensearch-project/OpenSearch/pull/20203))
- Allow setting index.creation_date on index creation and restore for plugin compatibility and migrations ([#19931](https://github.com/opensearch-project/OpenSearch/pull/19931))
- Add support for a ForkJoinPool type ([#19008](https://github.com/opensearch-project/OpenSearch/pull/19008))
- Add seperate shard limit validation for local and remote indices ([#19532](https://github.com/opensearch-project/OpenSearch/pull/19532))
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
- New cluster setting search.query.max_query_string_length_monitor_only ([#19539](https://github.com/opensearch-project/OpenSearch/pull/19539))
- Add a mapper for context aware segments grouping criteria ([#19233](https://github.com/opensearch-project/OpenSearch/pull/19233))
- Return full error for GRPC error response ([#19568](https://github.com/opensearch-project/OpenSearch/pull/19568))
- Add support for repository with Server side encryption enabled and client side encryption as well based on a flag. ([#19630)](https://github.com/opensearch-project/OpenSearch/pull/19630))
- Add pluggable gRPC interceptors with explicit ordering([#19005](https://github.com/opensearch-project/OpenSearch/pull/19005))
- Add BindableServices extension point to transport-grpc-spi ([#19304](https://github.com/opensearch-project/OpenSearch/pull/19304))
- Add metrics for the merged segment warmer feature ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
- Handle deleted documents for filter rewrite sub-aggregation optimization ([#19643](https://github.com/opensearch-project/OpenSearch/pull/19643))
- Add bulk collect API for filter rewrite sub-aggregation optimization ([#19933](https://github.com/opensearch-project/OpenSearch/pull/19933))
- Allow collectors take advantage of preaggregated data using collectRange API ([#20009](https://github.com/opensearch-project/OpenSearch/pull/20009))
- Bulk collection logic for metrics and cardinality aggregations ([#20067](https://github.com/opensearch-project/OpenSearch/pull/20067))
- Add pointer based lag metric in pull-based ingestion ([#19635](https://github.com/opensearch-project/OpenSearch/pull/19635))
- Introduced internal API for retrieving metadata about requested indices from transport actions  ([#18523](https://github.com/opensearch-project/OpenSearch/pull/18523))
- Add cluster defaults for merge autoThrottle, maxMergeThreads, and maxMergeCount; Add segment size filter to the merged segment warmer ([#19629](https://github.com/opensearch-project/OpenSearch/pull/19629))
- Add build-tooling to run in FIPS environment ([#18921](https://github.com/opensearch-project/OpenSearch/pull/18921))
- Add SMILE/CBOR/YAML document format support to Bulk GRPC endpoint ([#19744](https://github.com/opensearch-project/OpenSearch/pull/19744))
- Make test-suite runnable under FIPS compliance support ([#18491](https://github.com/opensearch-project/OpenSearch/pull/18491))
- Implement GRPC Search params `Highlight`and `Sort` ([#19868](https://github.com/opensearch-project/OpenSearch/pull/19868))
- Implement GRPC ConstantScoreQuery, FuzzyQuery, MatchBoolPrefixQuery, MatchPhrasePrefix, PrefixQuery, MatchQuery ([#19854](https://github.com/opensearch-project/OpenSearch/pull/19854))
- Add async periodic flush task support for pull-based ingestion ([#19878](https://github.com/opensearch-project/OpenSearch/pull/19878))
- Add support for context aware segments ([#19098](https://github.com/opensearch-project/OpenSearch/pull/19098))
- Implement GRPC FunctionScoreQuery ([#19888](https://github.com/opensearch-project/OpenSearch/pull/19888))
- Implement error_trace parameter for bulk requests ([#19985](https://github.com/opensearch-project/OpenSearch/pull/19985))
- Allow the truncate filter in normalizers ([#19778](https://github.com/opensearch-project/OpenSearch/issues/19778))
- Support pull-based ingestion message mappers and raw payload support ([#19765](https://github.com/opensearch-project/OpenSearch/pull/19765))
- Add search API tracker ([#18601](https://github.com/opensearch-project/OpenSearch/pull/18601))
- Support dynamic consumer configuration update in pull-based ingestion ([#19963](https://github.com/opensearch-project/OpenSearch/pull/19963))
- Cache the `StoredFieldsReader` for scroll query optimization ([#20112](https://github.com/opensearch-project/OpenSearch/pull/20112))


### Changed
- Handle custom metadata files in subdirectory-store ([#20157](https://github.com/opensearch-project/OpenSearch/pull/20157))
- Add support for missing proto fields in GRPC FunctionScore and Highlight ([#20169](https://github.com/opensearch-project/OpenSearch/pull/20169))
- Ensure all modules are included in INTEG_TEST testcluster distribution ([#20241](https://github.com/opensearch-project/OpenSearch/pull/20241))
- Cleanup HttpServerTransport.Dispatcher in Netty tests ([#20160](https://github.com/opensearch-project/OpenSearch/pull/20160))
- Add `cluster.initial_cluster_manager_nodes` to testClusters OVERRIDABLE_SETTINGS ([#20348](https://github.com/opensearch-project/OpenSearch/pull/20348))
- Add BigInteger support for unsigned_long fields in gRPC transport ([#20346](https://github.com/opensearch-project/OpenSearch/pull/20346))
- Install demo security information when running ./gradlew run -PinstalledPlugins="['opensearch-security']" ([#20372](https://github.com/opensearch-project/OpenSearch/pull/20372))

### Fixed
- Fix bug of warm index: FullFileCachedIndexInput was closed error ([#20055](https://github.com/opensearch-project/OpenSearch/pull/20055))
- Fix flaky test ClusterMaxMergesAtOnceIT.testClusterLevelDefaultUpdatesMergePolicy ([#18056](https://github.com/opensearch-project/OpenSearch/issues/18056))
- Fix bug in Assertion framework(Yaml Rest test): numeric comparison fails when comparing Integer vs Long (or Float vs Double) ([#19376](https://github.com/opensearch-project/OpenSearch/pull/19376))
- Fix Netty deprecation warnings in transport-netty4 module ([#20233](https://github.com/opensearch-project/OpenSearch/pull/20233))
- Fix snapshot restore when an index sort is present ([#20284](https://github.com/opensearch-project/OpenSearch/pull/20284))
- Fix SearchPhaseExecutionException to properly initCause ([#20320](https://github.com/opensearch-project/OpenSearch/pull/20320))
- Fix `cluster.remote.<cluster_alias>.server_name` setting no populating SNI ([#20321](https://github.com/opensearch-project/OpenSearch/pull/20321))

### Dependencies
- Bump `com.google.auth:google-auth-library-oauth2-http` from 1.38.0 to 1.41.0 ([#20183](https://github.com/opensearch-project/OpenSearch/pull/20183))
- Bump `actions/checkout` from 5 to 6 ([#20186](https://github.com/opensearch-project/OpenSearch/pull/20186))
- Bump `org.apache.commons:commons-configuration2` from 2.12.0 to 2.13.0 ([#20185](https://github.com/opensearch-project/OpenSearch/pull/20185), [#20184](https://github.com/opensearch-project/OpenSearch/pull/20184))
- Bump Project Reactor to 3.8.1 and Reactor Netty to 1.3.1 ([#20217](https://github.com/opensearch-project/OpenSearch/pull/20217))
- Bump OpenTelemetry to 1.57.0 and OpenTelemetry Semconv to 1.37.0 ([#20231](https://github.com/opensearch-project/OpenSearch/pull/20231))
- Bump `peter-evans/create-pull-request` from 7 to 8 ([#20236](https://github.com/opensearch-project/OpenSearch/pull/20236))
- Bump `org.apache.kerby:kerb-admin` from 2.1.0 to 2.1.1 ([#20235](https://github.com/opensearch-project/OpenSearch/pull/20235))
- Bump `org.checkerframework:checker-qual` from 3.49.0 to 3.52.1 ([#20234](https://github.com/opensearch-project/OpenSearch/pull/20234))
- Bump `netty` to 4.2.9.Final ([#20230](https://github.com/opensearch-project/OpenSearch/pull/20230))
- Bump `log4j` from 2.21.0 to 2.25.3 ([#20308](https://github.com/opensearch-project/OpenSearch/pull/20308))
- Bump `org.apache.logging.log4j:log4j-core` from 2.25.2 to 2.25.3 ([#20300](https://github.com/opensearch-project/OpenSearch/pull/20300))
- Bump `ch.qos.logback:logback-core` from 1.5.20 to 1.5.23 ([#20303](https://github.com/opensearch-project/OpenSearch/pull/20303))
- Bump `com.google.api.grpc:proto-google-iam-v1` from 1.57.0 to 1.58.2 ([#20302](https://github.com/opensearch-project/OpenSearch/pull/20302))
- Bump `actions/setup-java` from 4 to 5 ([#20304](https://github.com/opensearch-project/OpenSearch/pull/20304))
- Bump `com.squareup.okhttp3:okhttp` from 5.3.0 to 5.3.2 ([#20331](https://github.com/opensearch-project/OpenSearch/pull/20331))
- Bump `asm` from 9.7 to 9.9.1 ([#20330](https://github.com/opensearch-project/OpenSearch/pull/20330))
- Bump `actions/upload-artifact` from 5 to 6 ([#20333](https://github.com/opensearch-project/OpenSearch/pull/20333))
- Bump `com.google.http-client:google-http-client-appengine` from 2.0.2 to 2.0.3 ([#20332](https://github.com/opensearch-project/OpenSearch/pull/20332))
- Bump `jackson` from 2.18.1 to 2.20.1 ([#20343](https://github.com/opensearch-project/OpenSearch/pull/20343))
- Bump `opensearch-protobufs` from 0.24.0 to 1.0.0 and update transport-grpc module compatibility ([#20335](https://github.com/opensearch-project/OpenSearch/pull/20335))
- Bump Apache HttpClient5 to 5.6 ([#20358](https://github.com/opensearch-project/OpenSearch/pull/20358))
- Bump Apache HttpCore5 to 5.4 ([#20358](https://github.com/opensearch-project/OpenSearch/pull/20358))
- Bump `org.jline:jline` from 3.30.5 to 3.30.6 ([#20369](https://github.com/opensearch-project/OpenSearch/pull/20369))
- Bump `lycheeverse/lychee-action` from 2.6.1 to 2.7.0 ([#20370](https://github.com/opensearch-project/OpenSearch/pull/20370))

### Removed

- Remove identity-shiro from plugins folder ([#20305](https://github.com/opensearch-project/OpenSearch/pull/20305))

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.4...main
