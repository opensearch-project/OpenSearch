# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Expand fetch phase profiling to support inner hits and top hits aggregation phases  ([##18936](https://github.com/opensearch-project/OpenSearch/pull/18936))
- Add temporal routing processors for time-based document routing ([#18920](https://github.com/opensearch-project/OpenSearch/issues/18920))
- Implement Query Rewriting Infrastructure ([#19060](https://github.com/opensearch-project/OpenSearch/pull/19060))
- The dynamic mapping parameter supports false_allow_templates ([#19065](https://github.com/opensearch-project/OpenSearch/pull/19065))
- Add a toBuilder method in EngineConfig to support easy modification of configs([#19054](https://github.com/opensearch-project/OpenSearch/pull/19054))
- Add StoreFactory plugin interface for custom Store implementations([#19091](https://github.com/opensearch-project/OpenSearch/pull/19091))
- Use S3CrtClient for higher throughput while uploading files to S3 ([#18800](https://github.com/opensearch-project/OpenSearch/pull/18800))
- Add a dynamic setting to change skip_cache_factor and min_frequency for querycache ([#18351](https://github.com/opensearch-project/OpenSearch/issues/18351))
- Add overload constructor for Translog to accept Channel Factory as a parameter ([#18918](https://github.com/opensearch-project/OpenSearch/pull/18918))
- Add subdirectory-aware store module with recovery support ([#19132](https://github.com/opensearch-project/OpenSearch/pull/19132))
- Add a dynamic cluster setting to control the enablement of the merged segment warmer ([#18929](https://github.com/opensearch-project/OpenSearch/pull/18929))
### Changed
- Add CompletionStage variants to methods in the Client Interface and default to ActionListener impl ([#18998](https://github.com/opensearch-project/OpenSearch/pull/18998))
- IllegalArgumentException when scroll ID references a node not found in Cluster ([#19031](https://github.com/opensearch-project/OpenSearch/pull/19031))
- Adding ScriptedAvg class to painless spi to allowlist usage from plugins ([#19006](https://github.com/opensearch-project/OpenSearch/pull/19006))
- Replace centos:8 with almalinux:8 since centos docker images are deprecated ([#19154](https://github.com/opensearch-project/OpenSearch/pull/19154))
- Add CompletionStage variants to IndicesAdminClient as an alternative to ActionListener ([#19161](https://github.com/opensearch-project/OpenSearch/pull/19161))
- Remove cap on Java version used by forbidden APIs ([#19163](https://github.com/opensearch-project/OpenSearch/pull/19163))

### Fixed
- Fix unnecessary refreshes on update preparation failures ([#15261](https://github.com/opensearch-project/OpenSearch/issues/15261))
- Fix NullPointerException in segment replicator ([#18997](https://github.com/opensearch-project/OpenSearch/pull/18997))
- Ensure that plugins that utilize dumpCoverage can write to jacoco.dir when tests.security.manager is enabled ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))
- Fix OOM due to large number of shard result buffering ([#19066](https://github.com/opensearch-project/OpenSearch/pull/19066))
- Fix flaky tests in CloseIndexIT by addressing cluster state synchronization issues ([#18878](https://github.com/opensearch-project/OpenSearch/issues/18878))
- [Tiered Caching] Handle  query execution exception ([#19000](https://github.com/opensearch-project/OpenSearch/issues/19000))
- Grant access to testclusters dir for tests ([#19085](https://github.com/opensearch-project/OpenSearch/issues/19085))
- Fix assertion error when collapsing search results with concurrent segment search enabled ([#19053](https://github.com/opensearch-project/OpenSearch/pull/19053))
- Fix skip_unavailable setting changing to default during node drop issue ([#18766](https://github.com/opensearch-project/OpenSearch/pull/18766))
- Fix pull-based ingestion pause state initialization during replica promotion ([#19212](https://github.com/opensearch-project/OpenSearch/pull/19212))
- Fix QueryPhaseResultConsumer incomplete callback loops ([#19231](https://github.com/opensearch-project/OpenSearch/pull/19231))
- Fix the `scaled_float` precision issue ([#19188](https://github.com/opensearch-project/OpenSearch/pull/19188))

### Dependencies
- Bump `com.netflix.nebula.ospackage-base` from 12.0.0 to 12.1.0 ([#19019](https://github.com/opensearch-project/OpenSearch/pull/19019))
- Bump `actions/checkout` from 4 to 5 ([#19023](https://github.com/opensearch-project/OpenSearch/pull/19023))
- Bump `commons-cli:commons-cli` from 1.9.0 to 1.10.0 ([#19021](https://github.com/opensearch-project/OpenSearch/pull/19021))
- Bump `org.jline:jline` from 3.30.4 to 3.30.5 ([#19013](https://github.com/opensearch-project/OpenSearch/pull/19013))
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.3 to 4.9.4 ([#19015](https://github.com/opensearch-project/OpenSearch/pull/19015))
- Bump `com.azure:azure-storage-common` from 12.29.1 to 12.30.2 ([#19016](https://github.com/opensearch-project/OpenSearch/pull/19016), [#19145](https://github.com/opensearch-project/OpenSearch/pull/19145))
- Update OpenTelemetry to 1.53.0 and OpenTelemetry SemConv to 1.34.0 ([#19068](https://github.com/opensearch-project/OpenSearch/pull/19068))
- Bump `1password/load-secrets-action` from 2 to 3 ([#19100](https://github.com/opensearch-project/OpenSearch/pull/19100))
- Bump `com.nimbusds:nimbus-jose-jwt` from 10.3 to 10.4.2 ([#19099](https://github.com/opensearch-project/OpenSearch/pull/19099), [#19101](https://github.com/opensearch-project/OpenSearch/pull/19101))
- Bump netty from 4.1.121.Final to 4.1.124.Final ([#19103](https://github.com/opensearch-project/OpenSearch/pull/19103))
- Bump Google Cloud Storage SDK from 1.113.1 to 2.55.0 ([#18922](https://github.com/opensearch-project/OpenSearch/pull/18922))
- Bump `com.google.auth:google-auth-library-oauth2-http` from 1.37.1 to 1.38.0 ([#19144](https://github.com/opensearch-project/OpenSearch/pull/19144))
- Bump `com.squareup.okio:okio` from 3.15.0 to 3.16.0 ([#19146](https://github.com/opensearch-project/OpenSearch/pull/19146))
- Bump Slf4j from 1.7.36 to 2.0.17 ([#19136](https://github.com/opensearch-project/OpenSearch/pull/19136))
- Bump `org.apache.tika` from 2.9.2 to 3.2.2 ([#19125](https://github.com/opensearch-project/OpenSearch/pull/19125))
- Bump `org.apache.commons:commons-compress` from 1.26.1 to 1.28.0 ([#19125](https://github.com/opensearch-project/OpenSearch/pull/19125))
- Bump `io.projectreactor.netty:reactor_netty` from `1.2.5` to `1.2.9` ([#19222](https://github.com/opensearch-project/OpenSearch/pull/19222))
- Bump `org.bouncycastle:bouncycastle_jce` from `2.0.0` to `2.1.1` ([#19222](https://github.com/opensearch-project/OpenSearch/pull/19222))
- Bump `org.bouncycastle:bouncycastle_tls` from `2.0.20` to `2.1.20` ([#19222](https://github.com/opensearch-project/OpenSearch/pull/19222))
- Bump `org.bouncycastle:bouncycastle_pkix` from `2.0.8` to `2.1.9` ([#19222](https://github.com/opensearch-project/OpenSearch/pull/19222))
- Bump `org.bouncycastle:bouncycastle_pg` from `2.0.11` to `2.1.11` ([#19222](https://github.com/opensearch-project/OpenSearch/pull/19222))
- Bump `org.bouncycastle:bouncycastle_util` from `2.0.3` to `2.1.4` ([#19222](https://github.com/opensearch-project/OpenSearch/pull/19222))
- Bump `com.azure:azure-core` from 1.55.5 to 1.56.0 ([#19206](https://github.com/opensearch-project/OpenSearch/pull/19206))
- Bump `com.google.cloud:google-cloud-core` from 2.59.0 to 2.60.0 ([#19208](https://github.com/opensearch-project/OpenSearch/pull/19208))
- Bump `org.jsoup:jsoup` from 1.20.1 to 1.21.2 ([#19207](https://github.com/opensearch-project/OpenSearch/pull/19207))
- Bump `org.apache.hadoop:hadoop-minicluster` from 3.4.1 to 3.4.2 ([#19203](https://github.com/opensearch-project/OpenSearch/pull/19203))
- Bump `com.maxmind.geoip2:geoip2` from 4.3.1 to 4.4.0 ([#19205](https://github.com/opensearch-project/OpenSearch/pull/19205))

### Deprecated

### Removed
- Enable backward compatibility tests on Mac ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.2...main
