# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add useCompoundFile index setting ([#13478](https://github.com/opensearch-project/OpenSearch/pull/13478))
- Constant Keyword Field ([#12285](https://github.com/opensearch-project/OpenSearch/pull/12285))
- Convert ingest processor supports ip type ([#12818](https://github.com/opensearch-project/OpenSearch/pull/12818))
- Add a counter to node stat api to track shard going from idle to non-idle ([#12768](https://github.com/opensearch-project/OpenSearch/pull/12768))
- Allow setting KEYSTORE_PASSWORD through env variable ([#12865](https://github.com/opensearch-project/OpenSearch/pull/12865))
- [Concurrent Segment Search] Perform buildAggregation concurrently and support Composite Aggregations ([#12697](https://github.com/opensearch-project/OpenSearch/pull/12697))
- [Concurrent Segment Search] Disable concurrent segment search for system indices and throttled requests ([#12954](https://github.com/opensearch-project/OpenSearch/pull/12954))
- Rename ingest processor supports overriding target field if exists ([#12990](https://github.com/opensearch-project/OpenSearch/pull/12990))
- [Tiered Caching] Make took time caching policy setting dynamic ([#13063](https://github.com/opensearch-project/OpenSearch/pull/13063))
- Derived fields support to derive field values at query time without indexing ([#12569](https://github.com/opensearch-project/OpenSearch/pull/12569))
- Detect breaking changes on pull requests ([#9044](https://github.com/opensearch-project/OpenSearch/pull/9044))
- Add cluster primary balance contraint for rebalancing with buffer ([#12656](https://github.com/opensearch-project/OpenSearch/pull/12656))
- [Remote Store] Make translog transfer timeout configurable ([#12704](https://github.com/opensearch-project/OpenSearch/pull/12704))
- Reject Resize index requests (i.e, split, shrink and clone), While DocRep to SegRep migration is in progress.([#12686](https://github.com/opensearch-project/OpenSearch/pull/12686))
- Add support for more than one protocol for transport ([#12967](https://github.com/opensearch-project/OpenSearch/pull/12967))
- [Tiered Caching] Add dimension-based stats to ICache implementations. ([#12531](https://github.com/opensearch-project/OpenSearch/pull/12531))
- Add changes for overriding remote store and replication settings during snapshot restore. ([#11868](https://github.com/opensearch-project/OpenSearch/pull/11868))
- Add an individual setting of rate limiter for segment replication ([#12959](https://github.com/opensearch-project/OpenSearch/pull/12959))
- [Tiered Caching] Add dimension-based stats to TieredSpilloverCache ([#13236](https://github.com/opensearch-project/OpenSearch/pull/13236))
- [Tiered Caching] Expose new cache stats API ([#13237](https://github.com/opensearch-project/OpenSearch/pull/13237))
- [Streaming Indexing] Ensure support of the new transport by security plugin ([#13174](https://github.com/opensearch-project/OpenSearch/pull/13174))
- Add cluster setting to dynamically configure the buckets for filter rewrite optimization. ([#13179](https://github.com/opensearch-project/OpenSearch/pull/13179))
- Support wildcard/regex for indices param in _remotestore/_restore ([#8922](https://github.com/opensearch-project/OpenSearch/pull/8922))
- [Tiered Caching] Gate new stats logic behind FeatureFlags.PLUGGABLE_CACHE ([#13238](https://github.com/opensearch-project/OpenSearch/pull/13238))
- [Tiered Caching] Add a dynamic setting to disable/enable disk cache. ([#13373](https://github.com/opensearch-project/OpenSearch/pull/13373))
- [Remote Store] Add capability of doing refresh as determined by the translog ([#12992](https://github.com/opensearch-project/OpenSearch/pull/12992))
- [Batch Ingestion] Add `batch_size` to `_bulk` API. ([#12457](https://github.com/opensearch-project/OpenSearch/issues/12457))
- [Tiered caching] Make Indices Request Cache Stale Key Mgmt Threshold setting dynamic ([#12941](https://github.com/opensearch-project/OpenSearch/pull/12941))
- Batch mode for async fetching shard information in GatewayAllocator for unassigned shards ([#8746](https://github.com/opensearch-project/OpenSearch/pull/8746))
- [Remote Store] Add settings for remote path type and hash algorithm ([#13225](https://github.com/opensearch-project/OpenSearch/pull/13225))
- [Remote Store] Upload remote paths during remote enabled index creation ([#13386](https://github.com/opensearch-project/OpenSearch/pull/13386))
- [Search Pipeline] Handle default pipeline for multiple indices ([#13276](https://github.com/opensearch-project/OpenSearch/pull/13276))
- Add support for deep copying SearchRequest ([#12295](https://github.com/opensearch-project/OpenSearch/pull/12295))
- Support multi ranges traversal when doing date histogram rewrite optimization. ([#13317](https://github.com/opensearch-project/OpenSearch/pull/13317))

### Dependencies
- Bump `com.github.spullara.mustache.java:compiler` from 0.9.10 to 0.9.13 ([#13329](https://github.com/opensearch-project/OpenSearch/pull/13329), [#13559](https://github.com/opensearch-project/OpenSearch/pull/13559))
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))
- Bump `org.apache.commons:commons-text` from 1.11.0 to 1.12.0 ([#13557](https://github.com/opensearch-project/OpenSearch/pull/13557))

### Changed
- Add ability for Boolean and date field queries to run when only doc_values are enabled ([#11650](https://github.com/opensearch-project/OpenSearch/pull/11650))
- Refactor implementations of query phase searcher, allow QueryCollectorContext to have zero collectors ([#13481](https://github.com/opensearch-project/OpenSearch/pull/13481))
- Updated the `indices.query.bool.max_clause_count` setting from being static to dynamically updateable ([#13568](https://github.com/opensearch-project/OpenSearch/pull/13568))

### Deprecated

### Removed
- Remove handling of index.mapper.dynamic in AutoCreateIndex([#13067](https://github.com/opensearch-project/OpenSearch/pull/13067))

### Fixed
- Fix bulk API ignores ingest pipeline for upsert ([#12883](https://github.com/opensearch-project/OpenSearch/pull/12883))
- Fix issue with feature flags where default value may not be honored ([#12849](https://github.com/opensearch-project/OpenSearch/pull/12849))
- Fix UOE While building Exists query for nested search_as_you_type field ([#12048](https://github.com/opensearch-project/OpenSearch/pull/12048))
- Client with Java 8 runtime and Apache HttpClient 5 Transport fails with java.lang.NoSuchMethodError: java.nio.ByteBuffer.flip()Ljava/nio/ByteBuffer ([#13100](https://github.com/opensearch-project/opensearch-java/pull/13100))
- Fix from and size parameter can be negative when searching ([#13047](https://github.com/opensearch-project/OpenSearch/pull/13047))
- Enabled mockTelemetryPlugin for IT and fixed OOM issues ([#13054](https://github.com/opensearch-project/OpenSearch/pull/13054))
- Fix implement mark() and markSupported() in class FilterStreamInput ([#13098](https://github.com/opensearch-project/OpenSearch/pull/13098))
- Fix IndicesRequestCache Stale calculation ([#13070](https://github.com/opensearch-project/OpenSearch/pull/13070)]
- Fix snapshot _status API to return correct status for partial snapshots ([#12812](https://github.com/opensearch-project/OpenSearch/pull/12812))
- Improve the error messages for _stats with closed indices ([#13012](https://github.com/opensearch-project/OpenSearch/pull/13012))
- Ignore BaseRestHandler unconsumed content check as it's always consumed. ([#13290](https://github.com/opensearch-project/OpenSearch/pull/13290))
- Fix mapper_parsing_exception when using flat_object fields with names longer than 11 characters ([#13259](https://github.com/opensearch-project/OpenSearch/pull/13259))
- DATETIME_FORMATTER_CACHING_SETTING experimental feature should not default to 'true' ([#13532](https://github.com/opensearch-project/OpenSearch/pull/13532))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
