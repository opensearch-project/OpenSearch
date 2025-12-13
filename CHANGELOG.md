# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))

### Changed
- Handle custom metadata files in subdirectory-store ([#20157](https://github.com/opensearch-project/OpenSearch/pull/20157))
- Add support for missing proto fields in GRPC FunctionScore and Highlight ([#20169](https://github.com/opensearch-project/OpenSearch/pull/20169))
- Combining filter rewrite and skip list to optimize sub aggregation([#19573](https://github.com/opensearch-project/OpenSearch/pull/19573))
- Faster `terms` query creation for `keyword` field with index and docValues enabled ([#19350](https://github.com/opensearch-project/OpenSearch/pull/19350))
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))
- Omit maxScoreCollector in SimpleTopDocsCollectorContext when concurrent segment search enabled ([#19584](https://github.com/opensearch-project/OpenSearch/pull/19584))
- Onboarding new maven snapshots publishing to s3 ([#19619](https://github.com/opensearch-project/OpenSearch/pull/19619))
- Remove MultiCollectorWrapper and use MultiCollector in Lucene instead ([#19595](https://github.com/opensearch-project/OpenSearch/pull/19595))
- Change implementation for `percentiles` aggregation for latency improvement ([#19648](https://github.com/opensearch-project/OpenSearch/pull/19648))
- Wrap checked exceptions in painless.DefBootstrap to support JDK-25 ([#19706](https://github.com/opensearch-project/OpenSearch/pull/19706))
- Refactor the ThreadPoolStats.Stats class to use the Builder pattern instead of constructors ([#19317](https://github.com/opensearch-project/OpenSearch/pull/19317))
- Refactor the IndexingStats.Stats class to use the Builder pattern instead of constructors ([#19306](https://github.com/opensearch-project/OpenSearch/pull/19306))
- Remove FeatureFlag.MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG. ([#19715](https://github.com/opensearch-project/OpenSearch/pull/19715))
- Replace java.security.AccessController with org.opensearch.secure_sm.AccessController in sub projects with SocketAccess class ([#19803](https://github.com/opensearch-project/OpenSearch/pull/19803))
- Replace java.security.AccessController with org.opensearch.secure_sm.AccessController in discovery plugins ([#19802](https://github.com/opensearch-project/OpenSearch/pull/19802))
- Change the default value of doc_values in WildcardFieldMapper to true. ([#19796](https://github.com/opensearch-project/OpenSearch/pull/19796))
- Make Engine#loadHistoryUUID() protected and Origin#isFromTranslog() public ([#19753](https://github.com/opensearch-project/OpenSearch/pull/19752))
- Bump opensearch-protobufs dependency to 0.23.0 and update transport-grpc module compatibility ([#19831](https://github.com/opensearch-project/OpenSearch/pull/19831))
- Refactor the RefreshStats class to use the Builder pattern instead of constructors ([#19835](https://github.com/opensearch-project/OpenSearch/pull/19835))
- Refactor the DocStats and StoreStats class to use the Builder pattern instead of constructors ([#19863](https://github.com/opensearch-project/OpenSearch/pull/19863))
- Pass registry of headers from ActionPlugin.getRestHeaders to ActionPlugin.getRestHandlerWrapper ([#19875](https://github.com/opensearch-project/OpenSearch/pull/19875))
- Refactor the Condition.Stats and DirectoryFileTransferTracker.Stats class to use the Builder pattern instead of constructors ([#19862](https://github.com/opensearch-project/OpenSearch/pull/19862))
- Refactor the RemoteTranslogTransferTracker.Stats and RemoteSegmentTransferTracker.Stats class to use the Builder pattern instead of constructors ([#19837](https://github.com/opensearch-project/OpenSearch/pull/19837))
- Refactor the GetStats, FlushStats and QueryCacheStats class to use the Builder pattern instead of constructors ([#19935](https://github.com/opensearch-project/OpenSearch/pull/19935))
- Add RangeSemver for `dependencies` in `plugin-descriptor.properties` ([#19939](https://github.com/opensearch-project/OpenSearch/pull/19939))
- Refactor the FieldDataStats and CompletionStats class to use the Builder pattern instead of constructors ([#19936](https://github.com/opensearch-project/OpenSearch/pull/19936))
- Thread Context Preservation by gRPC Interceptor ([#19776](https://github.com/opensearch-project/OpenSearch/pull/19776))
- Update NoOpResult constructors in the Engine to be public ([#19950](https://github.com/opensearch-project/OpenSearch/pull/19950))
- Make XContentMapValues.filter case insensitive ([#19976](https://github.com/opensearch-project/OpenSearch/pull/19976))
- Refactor the TranslogStats and RequestCacheStats class to use the Builder pattern instead of constructors ([#19961](https://github.com/opensearch-project/OpenSearch/pull/19961))
- Improve performance of matrix_stats aggregation ([#19989](https://github.com/opensearch-project/OpenSearch/pull/19989))
- Refactor the IndexPressutreStats, DeviceStats and TransportStats class to use the Builder pattern instead of constructors ([#19991](https://github.com/opensearch-project/OpenSearch/pull/19991))
- Refactor the Cache.CacheStats class to use the Builder pattern instead of constructors ([#20015](https://github.com/opensearch-project/OpenSearch/pull/20015))
- Refactor the HttpStats, ScriptStats, AdaptiveSelectionStats and OsStats class to use the Builder pattern instead of constructors ([#20014](https://github.com/opensearch-project/OpenSearch/pull/20014))
- Bump opensearch-protobufs dependency to 0.24.0 and update transport-grpc module compatibility ([#20059](https://github.com/opensearch-project/OpenSearch/pull/20059))

- Refactor the ShardStats, WarmerStats and IndexingPressureStats class to use the Builder pattern instead of constructors ([#19966](https://github.com/opensearch-project/OpenSearch/pull/19966))
- Cleanup HttpServerTransport.Dispatcher in Netty tests ([#20160](https://github.com/opensearch-project/OpenSearch/pull/20160))

### Fixed
- Fix bug of warm index: FullFileCachedIndexInput was closed error ([#20055](https://github.com/opensearch-project/OpenSearch/pull/20055))
- Fix flaky test ClusterMaxMergesAtOnceIT.testClusterLevelDefaultUpdatesMergePolicy ([#18056](https://github.com/opensearch-project/OpenSearch/issues/18056))
- Fix bug in Assertion framework(Yaml Rest test): numeric comparison fails when comparing Integer vs Long (or Float vs Double) ([#19376](https://github.com/opensearch-project/OpenSearch/pull/19376))

### Dependencies
- Bump `com.google.auth:google-auth-library-oauth2-http` from 1.38.0 to 1.41.0 ([#20183](https://github.com/opensearch-project/OpenSearch/pull/20183))
- Bump `actions/checkout` from 5 to 6 ([#20186](https://github.com/opensearch-project/OpenSearch/pull/20186))
- Bump `org.apache.commons:commons-configuration2` from 2.12.0 to 2.13.0 ([#20185](https://github.com/opensearch-project/OpenSearch/pull/20185), [#20184](https://github.com/opensearch-project/OpenSearch/pull/20184))
- Bump Project Reactor to 3.8.1 and Reactor Netty to 1.3.1 ([#20217](https://github.com/opensearch-project/OpenSearch/pull/20217))

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.4...main
