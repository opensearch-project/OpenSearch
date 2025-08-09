# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- [Feature Request] Enhance Terms lookup query to support query clause instead of docId ([#18195](https://github.com/opensearch-project/OpenSearch/issues/18195))
- Add hierarchical routing processors for ingest and search pipelines ([#18826](https://github.com/opensearch-project/OpenSearch/pull/18826))
- Add ACL-aware routing processors for ingest and search pipelines ([#18834](https://github.com/opensearch-project/OpenSearch/pull/18834))
- Add temporal routing processors for time-based document routing ([#18920](https://github.com/opensearch-project/OpenSearch/issues/18920))
- Add support for Warm Indices Write Block on Flood Watermark breach ([#18375](https://github.com/opensearch-project/OpenSearch/pull/18375))
- FS stats for warm nodes based on addressable space ([#18767](https://github.com/opensearch-project/OpenSearch/pull/18767))
- Add support for custom index name resolver from cluster plugin ([#18593](https://github.com/opensearch-project/OpenSearch/pull/18593))
- Rename WorkloadGroupTestUtil to WorkloadManagementTestUtil ([#18709](https://github.com/opensearch-project/OpenSearch/pull/18709))
- Disallow resize for Warm Index, add Parameterized ITs for close in remote store ([#18686](https://github.com/opensearch-project/OpenSearch/pull/18686))
- Ability to run Code Coverage with Gradle and produce the jacoco reports locally ([#18509](https://github.com/opensearch-project/OpenSearch/issues/18509))
- Extend BooleanQuery must_not rewrite to numeric must, term, and terms queries ([#18498](https://github.com/opensearch-project/OpenSearch/pull/18498))
- [Workload Management] Update logging and Javadoc, rename QueryGroup to WorkloadGroup ([#18711](https://github.com/opensearch-project/OpenSearch/issues/18711))
- Add NodeResourceUsageStats to ClusterInfo ([#18480](https://github.com/opensearch-project/OpenSearch/issues/18472))
- Introduce SecureHttpTransportParameters experimental API (to complement SecureTransportParameters counterpart) ([#18572](https://github.com/opensearch-project/OpenSearch/issues/18572))
- Create equivalents of JSM's AccessController in the java agent ([#18346](https://github.com/opensearch-project/OpenSearch/issues/18346))
- [WLM] Add WLM mode validation for workload group CRUD requests ([#18652](https://github.com/opensearch-project/OpenSearch/issues/18652))
- Introduced a new cluster-level API to fetch remote store metadata (segments and translogs) for each shard of an index. ([#18257](https://github.com/opensearch-project/OpenSearch/pull/18257))
- Add last index request timestamp columns to the `_cat/indices` API. ([10766](https://github.com/opensearch-project/OpenSearch/issues/10766))
- Introduce a new pull-based ingestion plugin for file-based indexing (for local testing) ([#18591](https://github.com/opensearch-project/OpenSearch/pull/18591))
- Add support for search pipeline in search and msearch template ([#18564](https://github.com/opensearch-project/OpenSearch/pull/18564))
- [Workload Management] Modify logging message in WorkloadGroupService ([#18712](https://github.com/opensearch-project/OpenSearch/pull/18712))
- Add BooleanQuery rewrite moving constant-scoring must clauses to filter clauses ([#18510](https://github.com/opensearch-project/OpenSearch/issues/18510))
- Add functionality for plugins to inject QueryCollectorContext during QueryPhase ([#18637](https://github.com/opensearch-project/OpenSearch/pull/18637))
- Add support for non-timing info in profiler ([#18460](https://github.com/opensearch-project/OpenSearch/issues/18460))
- [Rule-based auto tagging] Bug fix and improvements ([#18726](https://github.com/opensearch-project/OpenSearch/pull/18726))
- Extend Approximation Framework to other numeric types ([#18530](https://github.com/opensearch-project/OpenSearch/issues/18530))
- Add Semantic Version field type mapper and extensive unit tests([#18454](https://github.com/opensearch-project/OpenSearch/pull/18454))
- Pass index settings to system ingest processor factories. ([#18708](https://github.com/opensearch-project/OpenSearch/pull/18708))
- Add fetch phase profiling. ([#18664](https://github.com/opensearch-project/OpenSearch/pull/18664))
- Include named queries from rescore contexts in matched_queries array ([#18697](https://github.com/opensearch-project/OpenSearch/pull/18697))
- Add the configurable limit on rule cardinality ([#18663](https://github.com/opensearch-project/OpenSearch/pull/18663))
- Optimization in Numeric Terms Aggregation query for Large Bucket Counts([#18702](https://github.com/opensearch-project/OpenSearch/pull/18702))
- Disable approximation framework when dealing with multiple sorts ([#18763](https://github.com/opensearch-project/OpenSearch/pull/18763))
- [Experimental] Start in "clusterless" mode if a clusterless ClusterPlugin is loaded ([#18479](https://github.com/opensearch-project/OpenSearch/pull/18479))
- [Star-Tree] Add star-tree search related stats ([#18707](https://github.com/opensearch-project/OpenSearch/pull/18707))
- Add support for plugins to profile information ([#18656](https://github.com/opensearch-project/OpenSearch/pull/18656))
- Add support for Combined Fields query ([#18724](https://github.com/opensearch-project/OpenSearch/pull/18724))
- Make GRPC transport extensible to allow plugins to register and expose their own GRPC services ([#18516](https://github.com/opensearch-project/OpenSearch/pull/18516))
- Added approximation support for range queries with now in date field ([#18511](https://github.com/opensearch-project/OpenSearch/pull/18511))
- Upgrade to protobufs 0.6.0 and clean up deprecated TermQueryProtoUtils code ([#18880](https://github.com/opensearch-project/OpenSearch/pull/18880))
- Expand fetch phase profiling to multi-shard queries ([#18887](https://github.com/opensearch-project/OpenSearch/pull/18887))
- Prevent shard initialization failure due to streaming consumer errors ([#18877](https://github.com/opensearch-project/OpenSearch/pull/18877))
- APIs for stream transport and new stream-based search api action ([#18722](https://github.com/opensearch-project/OpenSearch/pull/18722))
- Add support for custom remote store segment path prefix to support clusterless configurations ([#18750](https://github.com/opensearch-project/OpenSearch/issues/18750))
- Added the core process for warming merged segments in remote-store enabled domains ([#18683](https://github.com/opensearch-project/OpenSearch/pull/18683))
- Streaming aggregation ([#18874](https://github.com/opensearch-project/OpenSearch/pull/18874))
- Optimize Composite Aggregations by removing unnecessary object allocations ([#18531](https://github.com/opensearch-project/OpenSearch/pull/18531))
- [Star-Tree] Add search support for ip field type ([#18671](https://github.com/opensearch-project/OpenSearch/pull/18671))
- [Derived Source] Add integration of derived source feature across various paths like get/search/recovery ([#18565](https://github.com/opensearch-project/OpenSearch/pull/18565))
- Supporting Scripted Metric Aggregation when reducing aggregations in InternalValueCount and InternalAvg ([18411](https://github.com/opensearch-project/OpenSearch/pull18411)))
- Support `search_after` numeric queries with Approximation Framework ([#18896](https://github.com/opensearch-project/OpenSearch/pull/18896))
- Add skip_list parameter to Numeric Field Mappers (default false) ([#18889](https://github.com/opensearch-project/OpenSearch/pull/18889))
- Map to proper GRPC status codes and achieve exception handling parity with HTTP APIs([#18925](https://github.com/opensearch-project/OpenSearch/pull/18925))

### Changed

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
