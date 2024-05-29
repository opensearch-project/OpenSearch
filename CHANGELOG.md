# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add support for Azure Managed Identity in repository-azure ([#12423](https://github.com/opensearch-project/OpenSearch/issues/12423))
- Add useCompoundFile index setting ([#13478](https://github.com/opensearch-project/OpenSearch/pull/13478))
- Make outbound side of transport protocol dependent ([#13293](https://github.com/opensearch-project/OpenSearch/pull/13293))
- [Remote Store] Upload translog checkpoint as object metadata to translog.tlog([#13637](https://github.com/opensearch-project/OpenSearch/pull/13637))
- [Remote Store] Add dynamic cluster settings to set timeout for segments upload to Remote Store ([#13679](https://github.com/opensearch-project/OpenSearch/pull/13679))
- Add getMetadataFields to MapperService ([#13819](https://github.com/opensearch-project/OpenSearch/pull/13819))

### Dependencies
- Bump `com.github.spullara.mustache.java:compiler` from 0.9.10 to 0.9.13 ([#13329](https://github.com/opensearch-project/OpenSearch/pull/13329), [#13559](https://github.com/opensearch-project/OpenSearch/pull/13559))
- Bump `org.apache.commons:commons-text` from 1.11.0 to 1.12.0 ([#13557](https://github.com/opensearch-project/OpenSearch/pull/13557))
- Bump `org.hdrhistogram:HdrHistogram` from 2.1.12 to 2.2.1 ([#13556](https://github.com/opensearch-project/OpenSearch/pull/13556))
- Bump `com.gradle.enterprise` from 3.17.2 to 3.17.4 ([#13641](https://github.com/opensearch-project/OpenSearch/pull/13641), [#13753](https://github.com/opensearch-project/OpenSearch/pull/13753))
- Bump `org.apache.hadoop:hadoop-minicluster` from 3.3.6 to 3.4.0 ([#13642](https://github.com/opensearch-project/OpenSearch/pull/13642))
- Bump `mockito` from 5.11.0 to 5.12.0 ([#13665](https://github.com/opensearch-project/OpenSearch/pull/13665))
- Bump `com.google.code.gson:gson` from 2.10.1 to 2.11.0 ([#13752](https://github.com/opensearch-project/OpenSearch/pull/13752))
- Bump `ch.qos.logback:logback-core` from 1.5.3 to 1.5.6 ([#13756](https://github.com/opensearch-project/OpenSearch/pull/13756))
- Bump `netty` from 4.1.109.Final to 4.1.110.Final ([#13802](https://github.com/opensearch-project/OpenSearch/pull/13802))
- Bump `jackson` from 2.17.0 to 2.17.1 ([#13817](https://github.com/opensearch-project/OpenSearch/pull/13817))
- Bump `reactor` from 3.5.15 to 3.5.17 ([#13825](https://github.com/opensearch-project/OpenSearch/pull/13825))
- Bump `reactor-netty` from 1.1.17 to 1.1.19 ([#13825](https://github.com/opensearch-project/OpenSearch/pull/13825))
- Bump `commons-cli:commons-cli` from 1.7.0 to 1.8.0 ([#13840](https://github.com/opensearch-project/OpenSearch/pull/13840))
- Bump `org.apache.xmlbeans:xmlbeans` from 5.2.0 to 5.2.1 ([#13839](https://github.com/opensearch-project/OpenSearch/pull/13839))

### Changed
- Add ability for Boolean and date field queries to run when only doc_values are enabled ([#11650](https://github.com/opensearch-project/OpenSearch/pull/11650))
- Refactor implementations of query phase searcher, allow QueryCollectorContext to have zero collectors ([#13481](https://github.com/opensearch-project/OpenSearch/pull/13481))
- Adds support to inject telemetry instances to plugins ([#13636](https://github.com/opensearch-project/OpenSearch/pull/13636))

### Deprecated

### Removed
- Remove handling of index.mapper.dynamic in AutoCreateIndex([#13067](https://github.com/opensearch-project/OpenSearch/pull/13067))

### Fixed
- Fix get field mapping API returns 404 error in mixed cluster with multiple versions ([#13624](https://github.com/opensearch-project/OpenSearch/pull/13624))
- Allow clearing `remote_store.compatibility_mode` setting ([#13646](https://github.com/opensearch-project/OpenSearch/pull/13646))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
