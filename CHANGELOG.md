# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Expand fetch phase profiling to support inner hits and top hits aggregation phases  ([##18936](https://github.com/opensearch-project/OpenSearch/pull/18936))
- Add temporal routing processors for time-based document routing ([#18920](https://github.com/opensearch-project/OpenSearch/issues/18920))
- The dynamic mapping parameter supports false_allow_templates ([#19065](https://github.com/opensearch-project/OpenSearch/pull/19065))
- Add a toBuilder method in EngineConfig to support easy modification of configs([#19054](https://github.com/opensearch-project/OpenSearch/pull/19054))

### Changed
- Add CompletionStage variants to methods in the Client Interface and default to ActionListener impl ([#18998](https://github.com/opensearch-project/OpenSearch/pull/18998))
- IllegalArgumentException when scroll ID references a node not found in Cluster ([#19031](https://github.com/opensearch-project/OpenSearch/pull/19031))

### Fixed
- Fix unnecessary refreshes on update preparation failures ([#15261](https://github.com/opensearch-project/OpenSearch/issues/15261))
- Fix NullPointerException in segment replicator ([#18997](https://github.com/opensearch-project/OpenSearch/pull/18997))
- Ensure that plugins that utilize dumpCoverage can write to jacoco.dir when tests.security.manager is enabled ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))
- Fix OOM due to large number of shard result buffering ([#19066](https://github.com/opensearch-project/OpenSearch/pull/19066))

### Dependencies
- Bump `com.netflix.nebula.ospackage-base` from 12.0.0 to 12.1.0 ([#19019](https://github.com/opensearch-project/OpenSearch/pull/19019))
- Bump `actions/checkout` from 4 to 5 ([#19023](https://github.com/opensearch-project/OpenSearch/pull/19023))
- Bump `commons-cli:commons-cli` from 1.9.0 to 1.10.0 ([#19021](https://github.com/opensearch-project/OpenSearch/pull/19021))
- Bump `org.jline:jline` from 3.30.4 to 3.30.5 ([#19013](https://github.com/opensearch-project/OpenSearch/pull/19013))
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.3 to 4.9.4 ([#19015](https://github.com/opensearch-project/OpenSearch/pull/19015))
- Bump `com.azure:azure-storage-common` from 12.29.1 to 12.30.1 ([#19016](https://github.com/opensearch-project/OpenSearch/pull/19016))
- Update OpenTelemetry to 1.53.0 and OpenTelemetry SemConv to 1.34.0 ([#19068](https://github.com/opensearch-project/OpenSearch/pull/19068))

### Deprecated

### Removed
- Enable backward compatibility tests on Mac ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))

### Fixed
- Fix flaky tests in CloseIndexIT by addressing cluster state synchronization issues ([#18878](https://github.com/opensearch-project/OpenSearch/issues/18878))
- [Tiered Caching] Handle  query execution exception ([#19000](https://github.com/opensearch-project/OpenSearch/issues/19000))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
