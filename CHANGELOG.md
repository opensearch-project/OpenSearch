# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Admission control] Add enhancements to FS stats to include read/write time, queue size and IO time ([#10696](https://github.com/opensearch-project/OpenSearch/pull/10696))
- [Remote cluster state] Change file names for remote cluster state ([#10557](https://github.com/opensearch-project/OpenSearch/pull/10557))

### Dependencies
- Bumps jetty version to 9.4.52.v20230823 to fix GMS-2023-1857 ([#9822](https://github.com/opensearch-project/OpenSearch/pull/9822))
- Bump `netty` from 4.1.99.Final to 4.1.100.Final ([#10564](https://github.com/opensearch-project/OpenSearch/pull/10564))
- Bump Lucene from 9.7.0 to 9.8.0 ([10276](https://github.com/opensearch-project/OpenSearch/pull/10276))
- Bump `commons-io:commons-io` from 2.13.0 to 2.14.0 ([#10294](https://github.com/opensearch-project/OpenSearch/pull/10294))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.25.0 to 2.25.1 ([#10298](https://github.com/opensearch-project/OpenSearch/pull/10298))
- Bump `de.thetaphi:forbiddenapis` from 3.5.1 to 3.6 ([#10508](https://github.com/opensearch-project/OpenSearch/pull/10508))
- Bump OpenTelemetry from 1.30.1 to 1.31.0 ([#10617](https://github.com/opensearch-project/OpenSearch/pull/10617))
- Bump `org.codehaus.woodstox:stax2-api` from 4.2.1 to 4.2.2 ([#10639](https://github.com/opensearch-project/OpenSearch/pull/10639))
- Bump `org.bouncycastle:bc-fips` from 1.0.2.3 to 1.0.2.4 ([#10297](https://github.com/opensearch-project/OpenSearch/pull/10297))

### Changed
- Add the means to extract the contextual properties from HttpChannel, TcpCChannel and TrasportChannel without excessive typecasting ([#10562](https://github.com/opensearch-project/OpenSearch/pull/10562))
- Backport the PR #9107 for updating CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY setting to a dynamic setting ([#10606](https://github.com/opensearch-project/OpenSearch/pull/10606))
- [Remote Store] Add Remote Store backpressure rejection stats to `_nodes/stats` ([#10524](https://github.com/opensearch-project/OpenSearch/pull/10524))
- [BUG] Fix java.lang.SecurityException in repository-gcs plugin ([#10642](https://github.com/opensearch-project/OpenSearch/pull/10642))
- Add telemetry tracer/metric enable flag and integ test. ([#10395](https://github.com/opensearch-project/OpenSearch/pull/10395))

### Deprecated

### Removed

### Fixed
- Fix class_cast_exception when passing int to _version and other metadata fields in ingest simulate API ([#10101](https://github.com/opensearch-project/OpenSearch/pull/10101))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
