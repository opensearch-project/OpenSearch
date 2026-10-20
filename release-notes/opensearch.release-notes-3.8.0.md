## Version 3.8.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.8.0

### Features

* Add API to modify a data stream's backing indices (`POST /_data_stream/_modify`) ([#22487](https://github.com/opensearch-project/OpenSearch/pull/22487))
* Add `multivalue_doc_count` aggregation that returns the number of documents with two or more values ([#20472](https://github.com/opensearch-project/OpenSearch/pull/20472))
* Add pull-based ingestion from Hive tables via new `ingestion-hive` plugin ([#21700](https://github.com/opensearch-project/OpenSearch/pull/21700))
* Add support for HTTP/3 on the client side via JDK HttpClient ([#21718](https://github.com/opensearch-project/OpenSearch/pull/21718))
* Add gRPC support for Point-in-Time (PIT) create and delete operations ([#22105](https://github.com/opensearch-project/OpenSearch/pull/22105))
* Add gRPC API support for `extra_field_values` in bulk requests ([#21907](https://github.com/opensearch-project/OpenSearch/pull/21907))
* Add cross-cluster streaming support to the remote cluster client via Arrow Flight ([#22359](https://github.com/opensearch-project/OpenSearch/pull/22359))
* Add coordinator-side index-level field domain pruning before `can_match` for faster time-range queries ([#21865](https://github.com/opensearch-project/OpenSearch/pull/21865))
* Add bulkscorer script processing interface in ScriptScore query for bulk scoring optimizations ([#22423](https://github.com/opensearch-project/OpenSearch/pull/22423))
* Add process-wide ObjectInputFilter to reject Java deserialization by default, gated behind `bootstrap.serial_filter` setting ([#22073](https://github.com/opensearch-project/OpenSearch/pull/22073))
* Add cluster-level default setting for delayed shard allocation timeout (`cluster.routing.allocation.unassigned.node_left.delayed_timeout`) ([#22379](https://github.com/opensearch-project/OpenSearch/pull/22379))

### Enhancements

* Add HTTP request body decompression (gzip/deflate) to reactor-netty4 transport ([#22382](https://github.com/opensearch-project/OpenSearch/pull/22382))
* Add cross-setting validator to prevent cluster-manager crash loop when both balance factors are set to zero ([#22391](https://github.com/opensearch-project/OpenSearch/pull/22391))
* Add support for double, int, and long types in ExtraFieldValue for gRPC bulk indexing ([#22058](https://github.com/opensearch-project/OpenSearch/pull/22058))
* Add timing logs for repository create, update, and delete operations ([#22489](https://github.com/opensearch-project/OpenSearch/pull/22489))
* Add parent action name to `ProcessorGenerationContext` for conditional system-generated processor logic ([#22195](https://github.com/opensearch-project/OpenSearch/pull/22195))
* Add getter for remote translog transfer tracker to enable plugin access to tracking metrics ([#22453](https://github.com/opensearch-project/OpenSearch/pull/22453))
* Deprecate `RestClient` in favor of the internal HTTP client ([#22116](https://github.com/opensearch-project/OpenSearch/pull/22116))
* Disable Mustache partial template resolution in search templates ([#22438](https://github.com/opensearch-project/OpenSearch/pull/22438))
* Use binary serialization for resource usage headers to reduce CPU overhead on the search path ([#21230](https://github.com/opensearch-project/OpenSearch/pull/21230))
* Preserve resolved search pipeline id and inline pipeline source through query rewrite ([#22501](https://github.com/opensearch-project/OpenSearch/pull/22501))
* Fork `GET _aliases` serialization to MANAGEMENT thread pool to avoid blocking transport threads ([#21954](https://github.com/opensearch-project/OpenSearch/pull/21954))
* Strengthen `scroll_id` validation to prevent OOM from crafted scroll IDs ([#22396](https://github.com/opensearch-project/OpenSearch/pull/22396))
* Add depth tracking to recursive deserialization to prevent StackOverflowError ([#22404](https://github.com/opensearch-project/OpenSearch/pull/22404))
* Validate `base_path` in FsRepository to prevent path traversal outside `path.repo` ([#22328](https://github.com/opensearch-project/OpenSearch/pull/22328))
* Add path boundary validation in `resolveAnalyzerPath` to prevent directory traversal ([#22094](https://github.com/opensearch-project/OpenSearch/pull/22094))
* Register snapshot resilience settings (`snapshot.repository.io_timeout`, `max_outstanding_ops`, `cleanup_stale_blobs`) ([#22516](https://github.com/opensearch-project/OpenSearch/pull/22516))
* Wire `request_timeout` into S3 sync client to prevent indefinite hangs on degraded stores ([#22479](https://github.com/opensearch-project/OpenSearch/pull/22479))
* Separate internal ignore settings from user ignore settings during snapshot restore ([#20494](https://github.com/opensearch-project/OpenSearch/pull/20494))
* Validate `total_primary_shards_per_node` on index templates against the cluster instead of index-local flag ([#22203](https://github.com/opensearch-project/OpenSearch/pull/22203))
* Rollover `checkBlock` now scoped to write index only, skipping non-write alias members ([#21838](https://github.com/opensearch-project/OpenSearch/pull/21838))

### Bug Fixes

* Fix `OpenSearchTimeoutException` to return HTTP 504 instead of 500 ([#22064](https://github.com/opensearch-project/OpenSearch/pull/22064))
* Fix `ClassCastException` on malformed mappings in create index to return 400 instead of 500 ([#22371](https://github.com/opensearch-project/OpenSearch/pull/22371))
* Fix NPE for search-only indices without primaries in cluster allocation explain and bulk shard selection ([#22097](https://github.com/opensearch-project/OpenSearch/pull/22097))
* Fix NPE in S3 multipart upload when `provideStream()` throws, and fix shared segment array corruption ([#22309](https://github.com/opensearch-project/OpenSearch/pull/22309))
* Fix `ReplicationCheckpoint.compareTo` to return 0 for equal checkpoints, preventing TimSort crash during shard allocation ([#22376](https://github.com/opensearch-project/OpenSearch/pull/22376))
* Fix negative `os.cpu.percent` on cgroup v2 containers by granting read access to `/sys/fs/cgroup/cpu.stat` ([#22408](https://github.com/opensearch-project/OpenSearch/pull/22408))
* Fix snapshot listing failing on repositories containing legacy Elasticsearch snapshots ([#22193](https://github.com/opensearch-project/OpenSearch/pull/22193))
* Fix default SSE type to send `AES256` header, preventing silent snapshot failures on buckets requiring encryption ([#22144](https://github.com/opensearch-project/OpenSearch/pull/22144))
* Fix default replica count resolution from node settings (`opensearch.yml`) ([#22334](https://github.com/opensearch-project/OpenSearch/pull/22334))
* Fix `filter` rewrite crash with sub-aggregations in `date_histogram` when segment contains out-of-range documents ([#22390](https://github.com/opensearch-project/OpenSearch/pull/22390))
* Fix V1 template merge regression that broke dynamic object inheritance for dotted field paths ([#22515](https://github.com/opensearch-project/OpenSearch/pull/22515))
* Fix `disable_objects` array parsing to preserve dotted field names ([#22127](https://github.com/opensearch-project/OpenSearch/pull/22127))
* Fix field resolution for dotted fields under `disable_objects` ([#21929](https://github.com/opensearch-project/OpenSearch/pull/21929))
* Fix `NestedQueryBuilder.visit()` to recursively visit child query tree ([#22196](https://github.com/opensearch-project/OpenSearch/pull/22196))
* Fix `InputStream` leak when loading hyphenation patterns in analysis-common ([#22508](https://github.com/opensearch-project/OpenSearch/pull/22508))
* Fix `_cat/nodes` API showing negative values in cpu stats when perf counters are unavailable ([#22074](https://github.com/opensearch-project/OpenSearch/pull/22074))
* Fix global ordinals to properly trip fielddata circuit breaker ([#22129](https://github.com/opensearch-project/OpenSearch/pull/22129))
* Restore default max nesting depth to 1000 and decouple stream depth limit from XContent limit ([#22486](https://github.com/opensearch-project/OpenSearch/pull/22486))
* Fix unbounded recursion in deserialization that can cause StackOverflowError ([#22404](https://github.com/opensearch-project/OpenSearch/pull/22404))

### Maintenance

* Bump `bc-fips` to 2.1.3 to fix CVE-2026-8149 ([#22537](https://github.com/opensearch-project/OpenSearch/pull/22537))
* Upgrade Jackson 2 to 2.22.1 to resolve CVE-2026-54515 ([#22476](https://github.com/opensearch-project/OpenSearch/pull/22476))
* Bump Hadoop from 3.4.2 to 3.5.0 to resolve CVE-2026-2332 ([#22362](https://github.com/opensearch-project/OpenSearch/pull/22362))
* Update Jackson to 3.2.1 ([#22497](https://github.com/opensearch-project/OpenSearch/pull/22497))
* Update Netty to 4.2.16.Final ([#22403](https://github.com/opensearch-project/OpenSearch/pull/22403))
* Update OpenTelemetry to 1.63.0 ([#22111](https://github.com/opensearch-project/OpenSearch/pull/22111))
* Update Project Reactor to 3.8.6 and Reactor Netty to 1.3.6 ([#22059](https://github.com/opensearch-project/OpenSearch/pull/22059))
* Upgrade Lucene to 10.5.0 ([#22322](https://github.com/opensearch-project/OpenSearch/pull/22322))
* Accommodate JDK-26 related changes with respect to HttpClient behavior ([#22386](https://github.com/opensearch-project/OpenSearch/pull/22386))
* Remove `OpenSearchYamlFactory` / `OpenSearchYamlParser` after Jackson 3.2.0 update ([#22147](https://github.com/opensearch-project/OpenSearch/pull/22147))
