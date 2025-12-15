# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for forward translog reading ([#20163](https://github.com/opensearch-project/OpenSearch/pull/20163))
- Added public getter method in `SourceFieldMapper` to return excluded field ([#20205](https://github.com/opensearch-project/OpenSearch/pull/20205))

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
- Bump OpenTelemetry to 1.57.0 and OpenTelemetry Semconv to 1.37.0 ([#20231](https://github.com/opensearch-project/OpenSearch/pull/20231))
- Bump `org.apache.kerby:kerb-admin` from 2.1.0 to 2.1.1 ([#20235](https://github.com/opensearch-project/OpenSearch/pull/20235))
- Bump `org.checkerframework:checker-qual` from 3.49.0 to 3.52.1 ([#20234](https://github.com/opensearch-project/OpenSearch/pull/20234))

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.4...main

