# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for fields containing dots in their name as literals ([#19958](https://github.com/opensearch-project/OpenSearch/pull/19958))
- Add support for a ForkJoinPool type ([#19008](https://github.com/opensearch-project/OpenSearch/pull/19008))
- Add seperate shard limit validation for local and remote indices ([#19532](https://github.com/opensearch-project/OpenSearch/pull/19532))
- Use Lucene `pack` method for `half_float` and `usigned_long` when using `ApproximatePointRangeQuery`.
- Add a mapper for context aware segments grouping criteria ([#19233](https://github.com/opensearch-project/OpenSearch/pull/19233))
- Return full error for GRPC error response ([#19568](https://github.com/opensearch-project/OpenSearch/pull/19568))
- Add support for repository with Server side encryption enabled and client side encryption as well based on a flag. ([#19630)](https://github.com/opensearch-project/OpenSearch/pull/19630))
- Add pluggable gRPC interceptors with explicit ordering([#19005](https://github.com/opensearch-project/OpenSearch/pull/19005))
- Add BindableServices extension point to transport-grpc-spi ([#19304](https://github.com/opensearch-project/OpenSearch/pull/19304))
- Add metrics for the merged segment warmer feature ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
- Add pointer based lag metric in pull-based ingestion ([#19635](https://github.com/opensearch-project/OpenSearch/pull/19635))
- Introduced internal API for retrieving metadata about requested indices from transport actions  ([#18523](https://github.com/opensearch-project/OpenSearch/pull/18523))
- Add cluster defaults for merge autoThrottle, maxMergeThreads, and maxMergeCount; Add segment size filter to the merged segment warmer ([#19629](https://github.com/opensearch-project/OpenSearch/pull/19629))
- Add build-tooling to run in FIPS environment ([#18921](https://github.com/opensearch-project/OpenSearch/pull/18921))
- Add SMILE/CBOR/YAML document format support to Bulk GRPC endpoint ([#19744](https://github.com/opensearch-project/OpenSearch/pull/19744))
- Implement GRPC ConstantScoreQuery, FuzzyQuery, MatchBoolPrefixQuery, MatchPhrasePrefix, PrefixQuery, MatchQuery ([#19854](https://github.com/opensearch-project/OpenSearch/pull/19854))
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))

### Changed
- Handle custom metadata files in subdirectory-store ([#20157](https://github.com/opensearch-project/OpenSearch/pull/20157))
- Add support for missing proto fields in GRPC FunctionScore and Highlight ([#20169](https://github.com/opensearch-project/OpenSearch/pull/20169))

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
