# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add leader and follower check failure counter metrics ([#12439](https://github.com/opensearch-project/OpenSearch/pull/12439))
- Add latency metrics for instrumenting critical clusterManager code paths ([#12333](https://github.com/opensearch-project/OpenSearch/pull/12333))
- Add support for Azure Managed Identity in repository-azure ([#12423](https://github.com/opensearch-project/OpenSearch/issues/12423))
- Add useCompoundFile index setting ([#13478](https://github.com/opensearch-project/OpenSearch/pull/13478))
- Make outbound side of transport protocol dependent ([#13293](https://github.com/opensearch-project/OpenSearch/pull/13293))
- [Remote Store] Upload translog checkpoint as object metadata to translog.tlog([#13637](https://github.com/opensearch-project/OpenSearch/pull/13637))
- [Remote Store] Add dynamic cluster settings to set timeout for segments upload to Remote Store ([#13679](https://github.com/opensearch-project/OpenSearch/pull/13679))
- Add getMetadataFields to MapperService ([#13819](https://github.com/opensearch-project/OpenSearch/pull/13819))
- Add "wildcard" field type that supports efficient wildcard, prefix, and regexp queries ([#13461](https://github.com/opensearch-project/OpenSearch/pull/13461))
- Allow setting query parameters on requests ([#13776](https://github.com/opensearch-project/OpenSearch/issues/13776))
- Add dynamic action retry timeout setting ([#14022](https://github.com/opensearch-project/OpenSearch/issues/14022))
- Add capability to disable source recovery_source for an index ([#13590](https://github.com/opensearch-project/OpenSearch/pull/13590))
- Add remote routing table for remote state publication with experimental feature flag ([#13304](https://github.com/opensearch-project/OpenSearch/pull/13304))
- Add upload flow for writing routing table to remote store ([#13870](https://github.com/opensearch-project/OpenSearch/pull/13870))
- Add dynamic action retry timeout setting ([#14022](https://github.com/opensearch-project/OpenSearch/issues/14022))
- [Remote Store] Add support to disable flush based on translog reader count ([#14027](https://github.com/opensearch-project/OpenSearch/pull/14027))
- Add recovery chunk size setting ([#13997](https://github.com/opensearch-project/OpenSearch/pull/13997))
- [Query Insights] Add exporter support for top n queries ([#12982](https://github.com/opensearch-project/OpenSearch/pull/12982))
- [Query Insights] Add X-Opaque-Id to search request metadata for top n queries ([#13374](https://github.com/opensearch-project/OpenSearch/pull/13374))
- [Streaming Indexing] Enhance RestAction with request / response streaming support ([#13772](https://github.com/opensearch-project/OpenSearch/pull/13772))
- Move Remote Store Migration from DocRep to GA and modify remote migration settings name ([#14100](https://github.com/opensearch-project/OpenSearch/pull/14100))
- [Remote State] Add async remote state deletion task running on an interval, configurable by a setting ([#13995](https://github.com/opensearch-project/OpenSearch/pull/13995))
- Add remote routing table for remote state publication with experimental feature flag ([#13304](https://github.com/opensearch-project/OpenSearch/pull/13304))
- Add support for query level resource usage tracking ([#13172](https://github.com/opensearch-project/OpenSearch/pull/13172))
- [Query Insights] Add cpu and memory metrics to top n queries ([#13739](https://github.com/opensearch-project/OpenSearch/pull/13739))
- Derived field object type support ([#13720](https://github.com/opensearch-project/OpenSearch/pull/13720))

### Dependencies
- Bump `com.github.spullara.mustache.java:compiler` from 0.9.10 to 0.9.13 ([#13329](https://github.com/opensearch-project/OpenSearch/pull/13329), [#13559](https://github.com/opensearch-project/OpenSearch/pull/13559))
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
- Painless: ensure type "UnmodifiableMap" for params ([#13885](https://github.com/opensearch-project/OpenSearch/pull/13885))
- Don't return negative scores from `multi_match` query with `cross_fields` type  ([#13829](https://github.com/opensearch-project/OpenSearch/pull/13829))
- Pass parent filter to inner hit query ([#13903](https://github.com/opensearch-project/OpenSearch/pull/13903))
- Fix NPE on restore searchable snapshot ([#13911](https://github.com/opensearch-project/OpenSearch/pull/13911))
- Fix double invocation of postCollection when MultiBucketCollector is present ([#14015](https://github.com/opensearch-project/OpenSearch/pull/14015))
- Fix ReplicaShardBatchAllocator to batch shards without duplicates ([#13710](https://github.com/opensearch-project/OpenSearch/pull/13710))
- Java high-level REST client bulk() is not respecting the bulkRequest.requireAlias(true) method call ([#14146](https://github.com/opensearch-project/OpenSearch/pull/14146))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
