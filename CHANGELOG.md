# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- [Feature Request] Enhance Terms lookup query to support query clause instead of docId ([#18195](https://github.com/opensearch-project/OpenSearch/issues/18195))
- Add hierarchical routing processors for ingest and search pipelines ([#18826](https://github.com/opensearch-project/OpenSearch/pull/18826))
- Add ACL-aware routing processors for ingest and search pipelines ([#18834](https://github.com/opensearch-project/OpenSearch/pull/18834))
- Add support for Warm Indices Write Block on Flood Watermark breach ([#18375](https://github.com/opensearch-project/OpenSearch/pull/18375))
- FS stats for warm nodes based on addressable space ([#18767](https://github.com/opensearch-project/OpenSearch/pull/18767))
- Add support for custom index name resolver from cluster plugin ([#18593](https://github.com/opensearch-project/OpenSearch/pull/18593))
- Rename WorkloadGroupTestUtil to WorkloadManagementTestUtil ([#18709](https://github.com/opensearch-project/OpenSearch/pull/18709))
- Disallow resize for Warm Index, add Parameterized ITs for close in remote store ([#18686](https://github.com/opensearch-project/OpenSearch/pull/18686))
- Ability to run Code Coverage with Gradle and produce the jacoco reports locally ([#18509](https://github.com/opensearch-project/OpenSearch/issues/18509))
- Extend BooleanQuery must_not rewrite to numeric must, term, and terms queries ([#18498](https://github.com/opensearch-project/OpenSearch/pull/18498))
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
- Add QueryPhaseListener interface for pre/post collection hooks ([#17593](https://github.com/opensearch-project/OpenSearch/issues/17593))
- Add support for non-timing info in profiler ([#18460](https://github.com/opensearch-project/OpenSearch/issues/18460))
- [Rule-based auto tagging] Bug fix and improvements ([#18726](https://github.com/opensearch-project/OpenSearch/pull/18726))
- Extend Approximation Framework to other numeric types ([#18530](https://github.com/opensearch-project/OpenSearch/issues/18530))
- Add Semantic Version field type mapper and extensive unit tests([#18454](https://github.com/opensearch-project/OpenSearch/pull/18454))
- Pass index settings to system ingest processor factories. ([#18708](https://github.com/opensearch-project/OpenSearch/pull/18708))
- Add fetch phase profiling. ([#18664](https://github.com/opensearch-project/OpenSearch/pull/18664))
- Include named queries from rescore contexts in matched_queries array ([#18697](https://github.com/opensearch-project/OpenSearch/pull/18697))
- Add the configurable limit on rule cardinality ([#18663](https://github.com/opensearch-project/OpenSearch/pull/18663))
- Disable approximation framework when dealing with multiple sorts ([#18763](https://github.com/opensearch-project/OpenSearch/pull/18763))
- [Experimental] Start in "clusterless" mode if a clusterless ClusterPlugin is loaded ([#18479](https://github.com/opensearch-project/OpenSearch/pull/18479))
- [Star-Tree] Add star-tree search related stats ([#18707](https://github.com/opensearch-project/OpenSearch/pull/18707))
- Add support for plugins to profile information ([#18656](https://github.com/opensearch-project/OpenSearch/pull/18656))
- Add support for Combined Fields query ([#18724](https://github.com/opensearch-project/OpenSearch/pull/18724))
- Make GRPC transport extensible to allow plugins to register and expose their own GRPC services ([#18516](https://github.com/opensearch-project/OpenSearch/pull/18516))
- Added approximation support for range queries with now in date field ([#18511](https://github.com/opensearch-project/OpenSearch/pull/18511))
- Upgrade to protobufs 0.6.0 and clean up deprecated TermQueryProtoUtils code ([#18880](https://github.com/opensearch-project/OpenSearch/pull/18880))

### Changed
- Update Subject interface to use CheckedRunnable ([#18570](https://github.com/opensearch-project/OpenSearch/issues/18570))
- Update SecureAuxTransportSettingsProvider to distinguish between aux transport types ([#18616](https://github.com/opensearch-project/OpenSearch/pull/18616))
- Make node duress values cacheable ([#18649](https://github.com/opensearch-project/OpenSearch/pull/18649))
- Change default value of remote_data_ratio, which is used in Searchable Snapshots and Writeable Warm from 0 to 5 and min allowed value to 1 ([#18767](https://github.com/opensearch-project/OpenSearch/pull/18767))
- Making multi rate limiters in repository dynamic [#18069](https://github.com/opensearch-project/OpenSearch/pull/18069)
- Optimize grouping for segment concurrent search by ensuring that documents within each group are as equal as possible ([#18451](https://github.com/opensearch-project/OpenSearch/pull/18451))

### Dependencies
- Bump `stefanzweifel/git-auto-commit-action` from 5 to 6 ([#18524](https://github.com/opensearch-project/OpenSearch/pull/18524))
- Bump Apache Lucene to 10.2.2 ([#18573](https://github.com/opensearch-project/OpenSearch/pull/18573))
- Bump `org.apache.logging.log4j:log4j-core` from 2.24.3 to 2.25.1 ([#18589](https://github.com/opensearch-project/OpenSearch/pull/18589), [#18744](https://github.com/opensearch-project/OpenSearch/pull/18744))
- Bump `com.google.code.gson:gson` from 2.13.0 to 2.13.1 ([#18585](https://github.com/opensearch-project/OpenSearch/pull/18585))
- Bump `com.azure:azure-core-http-netty` from 1.15.11 to 1.15.12 ([#18586](https://github.com/opensearch-project/OpenSearch/pull/18586))
- Bump `com.squareup.okio:okio` from 3.13.0 to 3.15.0 ([#18645](https://github.com/opensearch-project/OpenSearch/pull/18645), [#18689](https://github.com/opensearch-project/OpenSearch/pull/18689))
- Bump `com.netflix.nebula.ospackage-base` from 11.11.2 to 12.0.0 ([#18646](https://github.com/opensearch-project/OpenSearch/pull/18646))
- Bump `com.azure:azure-storage-blob` from 12.30.0 to 12.30.1 ([#18644](https://github.com/opensearch-project/OpenSearch/pull/18644))
- Bump `com.google.guava:failureaccess` from 1.0.1 to 1.0.2 ([#18672](https://github.com/opensearch-project/OpenSearch/pull/18672))
- Bump `io.perfmark:perfmark-api` from 0.26.0 to 0.27.0 ([#18672](https://github.com/opensearch-project/OpenSearch/pull/18672))
- Bump `org.bouncycastle:bctls-fips` from 2.0.19 to 2.0.20 ([#18668](https://github.com/opensearch-project/OpenSearch/pull/18668))
- Bump `org.bouncycastle:bcpkix-fips` from 2.0.7 to 2.0.8 ([#18668](https://github.com/opensearch-project/OpenSearch/pull/18668))
- Bump `org.bouncycastle:bcpg-fips` from 2.0.10 to 2.0.11 ([#18668](https://github.com/opensearch-project/OpenSearch/pull/18668))
- Bump `com.password4j:password4j` from 1.8.2 to 1.8.3 ([#18668](https://github.com/opensearch-project/OpenSearch/pull/18668))
- Bump `com.azure:azure-core` from 1.55.3 to 1.55.5 ([#18691](https://github.com/opensearch-project/OpenSearch/pull/18691))
- Bump `com.squareup.okhttp3:okhttp` from 4.12.0 to 5.1.0 ([#18749](https://github.com/opensearch-project/OpenSearch/pull/18749))
- Bump `com.google.jimfs:jimfs` from 1.3.0 to 1.3.1 ([#18743](https://github.com/opensearch-project/OpenSearch/pull/18743)), [#18746](https://github.com/opensearch-project/OpenSearch/pull/18746)), [#18748](https://github.com/opensearch-project/OpenSearch/pull/18748))
- Bump `com.azure:azure-storage-common` from 12.29.0 to 12.29.1 ([#18742](https://github.com/opensearch-project/OpenSearch/pull/18742))
- Bump `org.apache.commons:commons-lang3` from 3.17.0 to 3.18.0 ([#18745](https://github.com/opensearch-project/OpenSearch/pull/18745))
- Bump `com.nimbusds:nimbus-jose-jwt` from 10.2 to 10.4 ([#18759](https://github.com/opensearch-project/OpenSearch/pull/18759), [#18804](https://github.com/opensearch-project/OpenSearch/pull/18804))
- Bump `commons-beanutils:commons-beanutils` from 1.9.4 to 1.11.0 ([#18401](https://github.com/opensearch-project/OpenSearch/issues/18401))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.7 to 1.1.10.8 ([#18803](https://github.com/opensearch-project/OpenSearch/pull/18803))

### Deprecated

### Removed

### Fixed
- Add task cancellation checks in aggregators ([#18426](https://github.com/opensearch-project/OpenSearch/pull/18426))
- Fix concurrent timings in profiler ([#18540](https://github.com/opensearch-project/OpenSearch/pull/18540))
- Fix regex query from query string query to work with field alias ([#18215](https://github.com/opensearch-project/OpenSearch/issues/18215))
- [Autotagging] Fix delete rule event consumption in InMemoryRuleProcessingService ([#18628](https://github.com/opensearch-project/OpenSearch/pull/18628))
- Cannot communicate with HTTP/2 when reactor-netty is enabled ([#18599](https://github.com/opensearch-project/OpenSearch/pull/18599))
- Fix the visit of sub queries for HasParentQuery and HasChildQuery ([#18621](https://github.com/opensearch-project/OpenSearch/pull/18621))
- Fix the backward compatibility regression with COMPLEMENT for Regexp queries introduced in OpenSearch 3.0 ([#18640](https://github.com/opensearch-project/OpenSearch/pull/18640))
- Fix Replication lag computation ([#18602](https://github.com/opensearch-project/OpenSearch/pull/18602))
- Fix max_score is null when sorting on score firstly ([#18715](https://github.com/opensearch-project/OpenSearch/pull/18715))
- Field-level ignore_malformed should override index-level setting ([#18706](https://github.com/opensearch-project/OpenSearch/pull/18706))
- Fixed Staggered merge -  load average replace with AverageTrackers, some Default thresholds modified ([#18666](https://github.com/opensearch-project/OpenSearch/pull/18666))
- Use `new SecureRandom()` to avoid blocking ([18729](https://github.com/opensearch-project/OpenSearch/issues/18729))
- Ignore awareness attributes when a custom preference string is included with a search request ([#18848](https://github.com/opensearch-project/OpenSearch/pull/18848))
- Use ScoreDoc instead of FieldDoc when creating TopScoreDocCollectorManager to avoid unnecessary conversion ([#18802](https://github.com/opensearch-project/OpenSearch/pull/18802))
- Fix leafSorter optimization for ReadOnlyEngine and NRTReplicationEngine ([#18639](https://github.com/opensearch-project/OpenSearch/pull/18639))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
