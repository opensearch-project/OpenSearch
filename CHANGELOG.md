# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
- Expand fetch phase profiling to support inner hits and top hits aggregation phases  ([##18936](https://github.com/opensearch-project/OpenSearch/pull/18936))
- [Rule-based Auto-tagging] add the schema for security attributes ([##19345](https://github.com/opensearch-project/OpenSearch/pull/19345))
- Add temporal routing processors for time-based document routing ([#18920](https://github.com/opensearch-project/OpenSearch/issues/18920))
- Implement Query Rewriting Infrastructure ([#19060](https://github.com/opensearch-project/OpenSearch/pull/19060))
- The dynamic mapping parameter supports false_allow_templates ([#19065](https://github.com/opensearch-project/OpenSearch/pull/19065) ([#19097](https://github.com/opensearch-project/OpenSearch/pull/19097)))
- [Rule-based Auto-tagging] restructure the in-memory trie to store values as a set ([#19344](https://github.com/opensearch-project/OpenSearch/pull/19344))
- Add a toBuilder method in EngineConfig to support easy modification of configs([#19054](https://github.com/opensearch-project/OpenSearch/pull/19054))
- Add StoreFactory plugin interface for custom Store implementations([#19091](https://github.com/opensearch-project/OpenSearch/pull/19091))
- Use S3CrtClient for higher throughput while uploading files to S3 ([#18800](https://github.com/opensearch-project/OpenSearch/pull/18800))
- [Rule-based Auto-tagging] bug fix on Update Rule API with multiple attributes ([#19497](https://github.com/opensearch-project/OpenSearch/pull/19497))
- Add a dynamic setting to change skip_cache_factor and min_frequency for querycache ([#18351](https://github.com/opensearch-project/OpenSearch/issues/18351))
- Add overload constructor for Translog to accept Channel Factory as a parameter ([#18918](https://github.com/opensearch-project/OpenSearch/pull/18918))
- Add subdirectory-aware store module with recovery support ([#19132](https://github.com/opensearch-project/OpenSearch/pull/19132))
- [Rule-based Auto-tagging] Modify get rule api to suit nested attributes ([#19429](https://github.com/opensearch-project/OpenSearch/pull/19429))
- [Rule-based Auto-tagging] Add autotagging label resolving logic for multiple attributes ([#19486](https://github.com/opensearch-project/OpenSearch/pull/19486))
- Field collapsing supports search_after ([#19261](https://github.com/opensearch-project/OpenSearch/pull/19261))
- Add a dynamic cluster setting to control the enablement of the merged segment warmer ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
- Publish transport-grpc-spi exposing QueryBuilderProtoConverter and QueryBuilderProtoConverterRegistry ([#18949](https://github.com/opensearch-project/OpenSearch/pull/18949))
- Support system generated search pipeline. ([#19128](https://github.com/opensearch-project/OpenSearch/pull/19128))
- Add `epoch_micros` date format ([#14669](https://github.com/opensearch-project/OpenSearch/issues/14669))
- Grok processor supports capturing multiple values for same field name ([#18799](https://github.com/opensearch-project/OpenSearch/pull/18799))
- Add support for search tie-breaking by _shard_doc ([#18924](https://github.com/opensearch-project/OpenSearch/pull/18924))
- Upgrade opensearch-protobufs dependency to 0.13.0 and update transport-grpc module compatibility ([#19007](https://github.com/opensearch-project/OpenSearch/issues/19007))
- Add new extensible method to DocRequest to specify type ([#19313](https://github.com/opensearch-project/OpenSearch/pull/19313))
- [Rule based auto-tagging] Add Rule based auto-tagging IT ([#18550](https://github.com/opensearch-project/OpenSearch/pull/18550))
- Add all-active ingestion as docrep equivalent in pull-based ingestion ([#19316](https://github.com/opensearch-project/OpenSearch/pull/19316))
- Adding logic for histogram aggregation using skiplist ([#19130](https://github.com/opensearch-project/OpenSearch/pull/19130))
- Add skip_list param for date, scaled float and token count fields ([#19142](https://github.com/opensearch-project/OpenSearch/pull/19142))
- Enable skip_list for @timestamp field or index sort field by default([#19480](https://github.com/opensearch-project/OpenSearch/pull/19480))
- Implement GRPC MatchPhrase, MultiMatch queries ([#19449](https://github.com/opensearch-project/OpenSearch/pull/19449))
- Optimize gRPC transport thread management for improved throughput ([#19278](https://github.com/opensearch-project/OpenSearch/pull/19278))
- Implement GRPC Boolean query and inject registry for all internal query converters ([#19391](https://github.com/opensearch-project/OpenSearch/pull/19391))
- Added precomputation for rare terms aggregation ([##18978](https://github.com/opensearch-project/OpenSearch/pull/18978))
- Implement GRPC Script query ([#19455](https://github.com/opensearch-project/OpenSearch/pull/19455))
- [Search Stats] Add search & star-tree search query failure count metrics ([#19210](https://github.com/opensearch-project/OpenSearch/issues/19210))
- [Star-tree] Support for multi-terms aggregation ([#18398](https://github.com/opensearch-project/OpenSearch/issues/18398))
- Add stream search enabled cluster setting and auto fallback logic ([#19506](https://github.com/opensearch-project/OpenSearch/pull/19506))
- Implement GRPC Exists, Regexp, and Wildcard queries ([#19392](https://github.com/opensearch-project/OpenSearch/pull/19392))
- Implement GRPC GeoBoundingBox, GeoDistance queries ([#19451](https://github.com/opensearch-project/OpenSearch/pull/19451))
- Implement GRPC Ids, Range, and Terms Set queries ([#19448](https://github.com/opensearch-project/OpenSearch/pull/19448))
- Implement GRPC Nested query ([#19453](https://github.com/opensearch-project/OpenSearch/pull/19453))
- Add sub aggregation support for histogram aggregation using skiplist ([19438](https://github.com/opensearch-project/OpenSearch/pull/19438))
- Optimization in String Terms Aggregation query for Large Bucket Counts([#18732](https://github.com/opensearch-project/OpenSearch/pull/18732))
- New cluster setting search.query.max_query_string_length ([#19491](https://github.com/opensearch-project/OpenSearch/pull/19491))
- Add `StreamNumericTermsAggregator` to allow numeric term aggregation streaming ([#19335](https://github.com/opensearch-project/OpenSearch/pull/19335))
- Harden the circuit breaker and failure handle logic in query result consumer ([#19396](https://github.com/opensearch-project/OpenSearch/pull/19396))
- Add pluggable gRPC interceptors with explicit ordering([#19005](https://github.com/opensearch-project/OpenSearch/pull/19005))

### Changed
- Refactor to move prepareIndex and prepareDelete methods to Engine class ([#19551](https://github.com/opensearch-project/OpenSearch/pull/19551))

### Fixed

### Dependencies
- Bump `org.apache.zookeeper:zookeeper` from 3.9.3 to 3.9.4 ([#19535](https://github.com/opensearch-project/OpenSearch/pull/19535))

### Deprecated

### Removed

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.3...main
