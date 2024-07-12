# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add fingerprint ingest processor ([#13724](https://github.com/opensearch-project/OpenSearch/pull/13724))
- [Remote Store] Rate limiter for remote store low priority uploads ([#14374](https://github.com/opensearch-project/OpenSearch/pull/14374/))
- Apply the date histogram rewrite optimization to range aggregation ([#13865](https://github.com/opensearch-project/OpenSearch/pull/13865))
- [Writable Warm] Add composite directory implementation and integrate it with FileCache ([12782](https://github.com/opensearch-project/OpenSearch/pull/12782))
- [Workload Management] Add QueryGroup schema ([13669](https://github.com/opensearch-project/OpenSearch/pull/13669))
- Add batching supported processor base type AbstractBatchingProcessor ([#14554](https://github.com/opensearch-project/OpenSearch/pull/14554))
- Fix race condition while parsing derived fields from search definition ([14445](https://github.com/opensearch-project/OpenSearch/pull/14445))
- Add `strict_allow_templates` dynamic mapping option ([#14555](https://github.com/opensearch-project/OpenSearch/pull/14555))
- Add allowlist setting for ingest-common and search-pipeline-common processors ([#14439](https://github.com/opensearch-project/OpenSearch/issues/14439))
- Create SystemIndexRegistry with helper method matchesSystemIndex ([#14415](https://github.com/opensearch-project/OpenSearch/pull/14415))
- Print reason why parent task was cancelled ([#14604](https://github.com/opensearch-project/OpenSearch/issues/14604))
- [Range Queries] Add new approximateable query framework to short-circuit range queries ([#13788](https://github.com/opensearch-project/OpenSearch/pull/13788))

### Dependencies
- Bump `com.github.spullara.mustache.java:compiler` from 0.9.10 to 0.9.13 ([#13329](https://github.com/opensearch-project/OpenSearch/pull/13329), [#13559](https://github.com/opensearch-project/OpenSearch/pull/13559))
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))
- Bump `org.apache.commons:commons-text` from 1.11.0 to 1.12.0 ([#13557](https://github.com/opensearch-project/OpenSearch/pull/13557))
- Bump `org.hdrhistogram:HdrHistogram` from 2.1.12 to 2.2.2 ([#13556](https://github.com/opensearch-project/OpenSearch/pull/13556), [#13986](https://github.com/opensearch-project/OpenSearch/pull/13986))
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
- Bump `actions/checkout` from 3 to 4 ([#13935](https://github.com/opensearch-project/OpenSearch/pull/13935))
- Bump `com.netflix.nebula.ospackage-base` from 11.9.0 to 11.9.1 ([#13933](https://github.com/opensearch-project/OpenSearch/pull/13933))
- Update to Apache Lucene 9.11.0 ([#14042](https://github.com/opensearch-project/OpenSearch/pull/14042))
- Bump `com.azure:azure-core-http-netty` from 1.12.8 to 1.15.1 ([#14128](https://github.com/opensearch-project/OpenSearch/pull/14128))
- Bump `tim-actions/get-pr-commits` from 1.1.0 to 1.3.1 ([#14126](https://github.com/opensearch-project/OpenSearch/pull/14126))

### Changed
- Add ability for Boolean and date field queries to run when only doc_values are enabled ([#11650](https://github.com/opensearch-project/OpenSearch/pull/11650))
- Refactor implementations of query phase searcher, allow QueryCollectorContext to have zero collectors ([#13481](https://github.com/opensearch-project/OpenSearch/pull/13481))
- Adds support to inject telemetry instances to plugins ([#13636](https://github.com/opensearch-project/OpenSearch/pull/13636))
- Adds support to provide tags with value in Gauge metric. ([#13994](https://github.com/opensearch-project/OpenSearch/pull/13994))
- Move cache removal notifications outside lru lock ([#14017](https://github.com/opensearch-project/OpenSearch/pull/14017))

### Deprecated

### Removed
- Remove handling of index.mapper.dynamic in AutoCreateIndex([#13067](https://github.com/opensearch-project/OpenSearch/pull/13067))

### Fixed
- Fix get field mapping API returns 404 error in mixed cluster with multiple versions ([#13624](https://github.com/opensearch-project/OpenSearch/pull/13624))
- Allow clearing `remote_store.compatibility_mode` setting ([#13646](https://github.com/opensearch-project/OpenSearch/pull/13646))
- Fix ReplicaShardBatchAllocator to batch shards without duplicates ([#13710](https://github.com/opensearch-project/OpenSearch/pull/13710))
- Don't return negative scores from `multi_match` query with `cross_fields` type  ([#13829](https://github.com/opensearch-project/OpenSearch/pull/13829))
- Painless: ensure type "UnmodifiableMap" for params ([#13885](https://github.com/opensearch-project/OpenSearch/pull/13885))
- Pass parent filter to inner hit query ([#13903](https://github.com/opensearch-project/OpenSearch/pull/13903))
- Fix NPE on restore searchable snapshot ([#13911](https://github.com/opensearch-project/OpenSearch/pull/13911))
- Fix double invocation of postCollection when MultiBucketCollector is present ([#14015](https://github.com/opensearch-project/OpenSearch/pull/14015))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
