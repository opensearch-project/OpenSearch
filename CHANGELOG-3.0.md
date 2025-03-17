# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679), [#10664](https://github.com/opensearch-project/OpenSearch/pull/10664))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- GHA to verify checklist items completion in PR descriptions ([#10800](https://github.com/opensearch-project/OpenSearch/pull/10800))
- [WLM] Add WLM support for search scroll API ([#16981](https://github.com/opensearch-project/OpenSearch/pull/16981))
- Allow to pass the list settings through environment variables (like [], ["a", "b", "c"], ...) ([#10625](https://github.com/opensearch-project/OpenSearch/pull/10625))
- Views, simplify data access and manipulation by providing a virtual layer over one or more indices ([#11957](https://github.com/opensearch-project/OpenSearch/pull/11957))
- Add systemd configurations to strengthen OS core security ([#17107](https://github.com/opensearch-project/OpenSearch/pull/17107))
- Added pull-based Ingestion (APIs, for ingestion source, a Kafka plugin, and IngestionEngine that pulls data from the ingestion source) ([#16958](https://github.com/opensearch-project/OpenSearch/pull/16958))
- Added ConfigurationUtils to core for the ease of configuration parsing [#17223](https://github.com/opensearch-project/OpenSearch/pull/17223)
- Add cluster and index level settings to limit the total primary shards per node and per index [#17295](https://github.com/opensearch-project/OpenSearch/pull/17295)
- Add execution_hint to cardinality aggregator request (#[17312](https://github.com/opensearch-project/OpenSearch/pull/17312))
- Arrow Flight RPC plugin with Flight server bootstrap logic and client for internode communication ([#16962](https://github.com/opensearch-project/OpenSearch/pull/16962))
- Added offset management for the pull-based Ingestion ([#17354](https://github.com/opensearch-project/OpenSearch/pull/17354))
- Added integ tests for systemd configs ([#17410](https://github.com/opensearch-project/OpenSearch/pull/17410))
- Add filter function for AbstractQueryBuilder, BoolQueryBuilder, ConstantScoreQueryBuilder([#17409](https://github.com/opensearch-project/OpenSearch/pull/17409))
- [Star Tree] [Search] Resolving keyword & numeric bucket aggregation with metric aggregation using star-tree ([#17165](https://github.com/opensearch-project/OpenSearch/pull/17165))
- Added error handling support for the pull-based ingestion ([#17427](https://github.com/opensearch-project/OpenSearch/pull/17427))
- Added Warm index setting and Updated nomenclature to differentiate between hot and warm tiering implementation ([#17490](https://github.com/opensearch-project/OpenSearch/pull/17490))


### Dependencies
- Update Apache Lucene to 10.1.0 ([#16366](https://github.com/opensearch-project/OpenSearch/pull/16366))
- Bump Apache HttpCore5/HttpClient5 dependencies from 5.2.5/5.3.1 to 5.3.1/5.4.1 to support ExtendedSocketOption in HttpAsyncClient ([#16757](https://github.com/opensearch-project/OpenSearch/pull/16757))
- Bumps `jetty` version from 9.4.55.v20240627 to 9.4.57.v20241219
- Switch main/3.x to use JDK21 LTS version ([#17515](https://github.com/opensearch-project/OpenSearch/pull/17515))

### Changed
- Changed locale provider from COMPAT to CLDR  ([#14345](https://github.com/opensearch-project/OpenSearch/pull/14345))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))
- Add task completion count in search backpressure stats API ([#10028](https://github.com/opensearch-project/OpenSearch/pull/10028/))
- Deprecate CamelCase `PathHierarchy` tokenizer name in favor to lowercase `path_hierarchy` ([#10894](https://github.com/opensearch-project/OpenSearch/pull/10894))
- Breaking change: Do not request "search_pipelines" metrics by default in NodesInfoRequest ([#12497](https://github.com/opensearch-project/OpenSearch/pull/12497))
- Use simpler matching logic for source fields when explicit field names (no wildcards or dot-paths) are specified ([#17160](https://github.com/opensearch-project/OpenSearch/pull/17160))
- Refactor `:libs` module `bootstrap` package to eliminate top level split packages for JPMS support ([#17117](https://github.com/opensearch-project/OpenSearch/pull/17117))
- Refactor the codebase to eliminate top level split packages for JPMS support ([#17153](https://github.com/opensearch-project/OpenSearch/pull/17153)
- Refactor `:server` module `org.apacge.lucene` package to eliminate top level split packages for JPMS support ([#17241](https://github.com/opensearch-project/OpenSearch/pull/17241))
- Stop minimizing automata used for case-insensitive matches ([#17268](https://github.com/opensearch-project/OpenSearch/pull/17268))
- Refactor the `:server` module `org.opensearch.client` to `org.opensearch.transport.client` to eliminate top level split packages for JPMS support ([#17272](https://github.com/opensearch-project/OpenSearch/pull/17272))
- Use Lucene `BM25Similarity` as default since the `LegacyBM25Similarity` is marked as deprecated ([#17306](https://github.com/opensearch-project/OpenSearch/pull/17306))
- Wildcard field index only 3gram of the input data [#17349](https://github.com/opensearch-project/OpenSearch/pull/17349)
- Use BC libraries to parse PEM files, increase key length, allow general use of known cryptographic binary extensions, remove unused BC dependencies ([#3420](https://github.com/opensearch-project/OpenSearch/pull/14912))
- Add optional enum set read / write functionality to stream input / output ([#17556](https://github.com/opensearch-project/OpenSearch/pull/17556))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))
- Remove custom Map, List and Set collection classes ([#6871](https://github.com/opensearch-project/OpenSearch/pull/6871))
- Remove `index.store.hybrid.mmap.extensions` setting in favor of `index.store.hybrid.nio.extensions` setting ([#9392](https://github.com/opensearch-project/OpenSearch/pull/9392))
- Remove package org.opensearch.action.support.master ([#4856](https://github.com/opensearch-project/OpenSearch/issues/4856))
- Remove transport-nio plugin ([#16887](https://github.com/opensearch-project/OpenSearch/issues/16887))
- Remove deprecated 'gateway' settings used to defer cluster recovery ([#3117](https://github.com/opensearch-project/OpenSearch/issues/3117))
- Remove FeatureFlags.PLUGGABLE_CACHE as the feature is no longer experimental ([#17344](https://github.com/opensearch-project/OpenSearch/pull/17344))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Don't over-allocate in HeapBufferedAsyncEntityConsumer in order to consume the response ([#9993](https://github.com/opensearch-project/OpenSearch/pull/9993))
- Fix swapped field formats in nodes API where `total_indexing_buffer_in_bytes` and `total_indexing_buffer` values were reversed ([#17070](https://github.com/opensearch-project/OpenSearch/pull/17070))
- Add HTTP/2 protocol support to HttpRequest.HttpVersion ([#17248](https://github.com/opensearch-project/OpenSearch/pull/17248))
- Fix missing bucket in terms aggregation with missing value ([#17418](https://github.com/opensearch-project/OpenSearch/pull/17418))
- Fix explain action on query rewrite ([#17286](https://github.com/opensearch-project/OpenSearch/pull/17286))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
