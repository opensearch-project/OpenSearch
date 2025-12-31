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

- Allow setting index.creation_date on index creation and restore for plugin compatibility and migrations ([#19931](https://github.com/opensearch-project/OpenSearch/pull/19931))
- Add support for a ForkJoinPool type ([#19008](https://github.com/opensearch-project/OpenSearch/pull/19008))
- Add seperate shard limit validation for local and remote indices ([#19532](https://github.com/opensearch-project/OpenSearch/pull/19532))
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
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
- Implement GRPC Search params `Highlight`and `Sort` ([#19868](https://github.com/opensearch-project/OpenSearch/pull/19868))
- Implement GRPC ConstantScoreQuery, FuzzyQuery, MatchBoolPrefixQuery, MatchPhrasePrefix, PrefixQuery, MatchQuery ([#19854](https://github.com/opensearch-project/OpenSearch/pull/19854))
- Add async periodic flush task support for pull-based ingestion ([#19878](https://github.com/opensearch-project/OpenSearch/pull/19878))
- Add support for context aware segments ([#19098](https://github.com/opensearch-project/OpenSearch/pull/19098))
- Implement GRPC FunctionScoreQuery ([#19888](https://github.com/opensearch-project/OpenSearch/pull/19888))
- Implement error_trace parameter for bulk requests ([#19985](https://github.com/opensearch-project/OpenSearch/pull/19985))
- Allow the truncate filter in normalizers ([#19778](https://github.com/opensearch-project/OpenSearch/issues/19778))
- Support pull-based ingestion message mappers and raw payload support ([#19765](https://github.com/opensearch-project/OpenSearch/pull/19765))
- Support dynamic consumer configuration update in pull-based ingestion ([#19963](https://github.com/opensearch-project/OpenSearch/pull/19963))
- Choose the best performing node when writing with append only index ([#20065](https://github.com/opensearch-project/OpenSearch/pull/20065))

### Changed

### Fixed

### Dependencies

### Removed

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.5...main
