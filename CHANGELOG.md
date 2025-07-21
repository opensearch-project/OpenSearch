# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Expand fetch phase profiling to support inner hits and top hits aggregation phases  ([##18936](https://github.com/opensearch-project/OpenSearch/pull/18936))
- Add temporal routing processors for time-based document routing ([#18920](https://github.com/opensearch-project/OpenSearch/issues/18920))
- The dynamic mapping parameter supports false_allow_templates ([#19065](https://github.com/opensearch-project/OpenSearch/pull/19065))
- Add a toBuilder method in EngineConfig to support easy modification of configs([#19054](https://github.com/opensearch-project/OpenSearch/pull/19054))
- Add StoreFactory plugin interface for custom Store implementations([#19091](https://github.com/opensearch-project/OpenSearch/pull/19091))
- Add a dynamic setting to change skip_cache_factor and min_frequency for querycache ([#18351](https://github.com/opensearch-project/OpenSearch/issues/18351))

- Add support for Warm Indices Write Block on Flood Watermark breach ([#18375](https://github.com/opensearch-project/OpenSearch/pull/18375))
- Add support for custom index name resolver from cluster plugin ([#18593](https://github.com/opensearch-project/OpenSearch/pull/18593))
- Rename WorkloadGroupTestUtil to WorkloadManagementTestUtil ([#18709](https://github.com/opensearch-project/OpenSearch/pull/18709))
- Disallow resize for Warm Index, add Parameterized ITs for close in remote store ([#18686](https://github.com/opensearch-project/OpenSearch/pull/18686))
- Ability to run Code Coverage with Gradle and produce the jacoco reports locally ([#18509](https://github.com/opensearch-project/OpenSearch/issues/18509))
- [Workload Management] Update logging and Javadoc, rename QueryGroup to WorkloadGroup ([#18711](https://github.com/opensearch-project/OpenSearch/issues/18711))
- Add NodeResourceUsageStats to ClusterInfo ([#18480](https://github.com/opensearch-project/OpenSearch/issues/18472))
- Introduce SecureHttpTransportParameters experimental API (to complement SecureTransportParameters counterpart) ([#18572](https://github.com/opensearch-project/OpenSearch/issues/18572))
- Create equivalents of JSM's AccessController in the java agent ([#18346](https://github.com/opensearch-project/OpenSearch/issues/18346))
- [WLM] Add WLM mode validation for workload group CRUD requests ([#18652](https://github.com/opensearch-project/OpenSearch/issues/18652))
- Introduced a new cluster-level API to fetch remote store metadata (segments and translogs) for each shard of an index. ([#18257](https://github.com/opensearch-project/OpenSearch/pull/18257))
- Add last index request timestamp columns to the `_cat/indices` API. ([10766](https://github.com/opensearch-project/OpenSearch/issues/10766))
- Introduce a new pull-based ingestion plugin for file-based indexing (for local testing) ([#18591](https://github.com/opensearch-project/OpenSearch/pull/18591))
- Add support for search pipeline in search and msearch template ([#18564](https://github.com/opensearch-project/OpenSearch/pull/18564))
- [Workload Management] Modify logging message in WorkloadGroupService ([#18712](https://github.com/opensearch-project/OpenSearch/pull/18712))
- Add BooleanQuery rewrite moving constant-scoring must clauses to filter clauses ([#18510](https://github.com/opensearch-project/OpenSearch/issues/18510))
- Add functionality for plugins to inject QueryCollectorContext during QueryPhase ([#18637](https://github.com/opensearch-project/OpenSearch/pull/18637))
- Add support for non-timing info in profiler ([#18460](https://github.com/opensearch-project/OpenSearch/issues/18460))
- [Rule-based auto tagging] Bug fix and improvements ([#18726](https://github.com/opensearch-project/OpenSearch/pull/18726))
- Extend Approximation Framework to other numeric types ([#18530](https://github.com/opensearch-project/OpenSearch/issues/18530))
- Add Semantic Version field type mapper and extensive unit tests([#18454](https://github.com/opensearch-project/OpenSearch/pull/18454))
- Pass index settings to system ingest processor factories. ([#18708](https://github.com/opensearch-project/OpenSearch/pull/18708))
- Include named queries from rescore contexts in matched_queries array ([#18697](https://github.com/opensearch-project/OpenSearch/pull/18697))
- Add the configurable limit on rule cardinality ([#18663](https://github.com/opensearch-project/OpenSearch/pull/18663))
- [Experimental] Start in "clusterless" mode if a clusterless ClusterPlugin is loaded ([#18479](https://github.com/opensearch-project/OpenSearch/pull/18479))
- [Star-Tree] Add star-tree search related stats ([#18707](https://github.com/opensearch-project/OpenSearch/pull/18707))
- Add support for plugins to profile information ([#18656](https://github.com/opensearch-project/OpenSearch/pull/18656))
- Add support for Combined Fields query ([#18724](https://github.com/opensearch-project/OpenSearch/pull/18724))
- Use S3CrtClient for higher throughput while uploading files to S3 ([#18779](https://github.com/opensearch-project/OpenSearch/pull/18779))

### Changed
- Add CompletionStage variants to methods in the Client Interface and default to ActionListener impl ([#18998](https://github.com/opensearch-project/OpenSearch/pull/18998))
- IllegalArgumentException when scroll ID references a node not found in Cluster ([#19031](https://github.com/opensearch-project/OpenSearch/pull/19031))
- Adding ScriptedAvg class to painless spi to allowlist usage from plugins ([#19006](https://github.com/opensearch-project/OpenSearch/pull/19006))

### Fixed
- Fix unnecessary refreshes on update preparation failures ([#15261](https://github.com/opensearch-project/OpenSearch/issues/15261))
- Fix NullPointerException in segment replicator ([#18997](https://github.com/opensearch-project/OpenSearch/pull/18997))
- Ensure that plugins that utilize dumpCoverage can write to jacoco.dir when tests.security.manager is enabled ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))
- Fix OOM due to large number of shard result buffering ([#19066](https://github.com/opensearch-project/OpenSearch/pull/19066))
- Fix flaky tests in CloseIndexIT by addressing cluster state synchronization issues ([#18878](https://github.com/opensearch-project/OpenSearch/issues/18878))
- [Tiered Caching] Handle  query execution exception ([#19000](https://github.com/opensearch-project/OpenSearch/issues/19000))
- Grant access to testclusters dir for tests ([#19085](https://github.com/opensearch-project/OpenSearch/issues/19085))
- Fix skip_unavailable setting changing to default during node drop issue ([#18766](https://github.com/opensearch-project/OpenSearch/pull/18766))

### Dependencies
- Bump `com.netflix.nebula.ospackage-base` from 12.0.0 to 12.1.0 ([#19019](https://github.com/opensearch-project/OpenSearch/pull/19019))
- Bump `actions/checkout` from 4 to 5 ([#19023](https://github.com/opensearch-project/OpenSearch/pull/19023))
- Bump `commons-cli:commons-cli` from 1.9.0 to 1.10.0 ([#19021](https://github.com/opensearch-project/OpenSearch/pull/19021))
- Bump `org.jline:jline` from 3.30.4 to 3.30.5 ([#19013](https://github.com/opensearch-project/OpenSearch/pull/19013))
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.3 to 4.9.4 ([#19015](https://github.com/opensearch-project/OpenSearch/pull/19015))
- Bump `com.azure:azure-storage-common` from 12.29.1 to 12.30.1 ([#19016](https://github.com/opensearch-project/OpenSearch/pull/19016))
- Update OpenTelemetry to 1.53.0 and OpenTelemetry SemConv to 1.34.0 ([#19068](https://github.com/opensearch-project/OpenSearch/pull/19068))
- Bump `1password/load-secrets-action` from 2 to 3 ([#19100](https://github.com/opensearch-project/OpenSearch/pull/19100))
- Bump `com.nimbusds:nimbus-jose-jwt` from 10.3 to 10.4.2 ([#19099](https://github.com/opensearch-project/OpenSearch/pull/19099), [#19101](https://github.com/opensearch-project/OpenSearch/pull/19101))
- Bump netty from 4.1.121.Final to 4.1.124.Final ([#19103](https://github.com/opensearch-project/OpenSearch/pull/19103))
- Bump google cloud storage from 1.113.1 to 2.55.0 ([#4547](https://github.com/opensearch-project/OpenSearch/pull/4547))

### Deprecated

### Removed
- Enable backward compatibility tests on Mac ([#18983](https://github.com/opensearch-project/OpenSearch/pull/18983))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
