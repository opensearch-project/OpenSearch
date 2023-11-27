# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Admission control] Add Resource usage collector service and resource usage tracker ([#10695](https://github.com/opensearch-project/OpenSearch/pull/10695))
- [Admission control] Add enhancements to FS stats to include read/write time, queue size and IO time ([#10696](https://github.com/opensearch-project/OpenSearch/pull/10696))
- [Remote cluster state] Change file names for remote cluster state ([#10557](https://github.com/opensearch-project/OpenSearch/pull/10557))
- [Remote Store] Add repository stats for remote store([#10567](https://github.com/opensearch-project/OpenSearch/pull/10567))
- [Remote cluster state] Upload global metadata in cluster state to remote store([#10404](https://github.com/opensearch-project/OpenSearch/pull/10404))
- [Remote cluster state] Download functionality of global metadata from remote store ([#10535](https://github.com/opensearch-project/OpenSearch/pull/10535))
- [Remote cluster state] Restore global metadata from remote store when local state is lost after quorum loss ([#10404](https://github.com/opensearch-project/OpenSearch/pull/10404))
- [Remote cluster state] Make index and global metadata upload timeout dynamic cluster settings ([#10814](https://github.com/opensearch-project/OpenSearch/pull/10814))
- Add search query categorizor ([#10255](https://github.com/opensearch-project/OpenSearch/pull/10255))
- Per request phase latency ([#10351](https://github.com/opensearch-project/OpenSearch/issues/10351))
- Add cluster state stats ([#10670](https://github.com/opensearch-project/OpenSearch/pull/10670))
- [Remote cluster state] Restore cluster state version during remote state auto restore ([#10853](https://github.com/opensearch-project/OpenSearch/pull/10853))
- Update the indexRandom function to create more segments for concurrent search tests ([10247](https://github.com/opensearch-project/OpenSearch/pull/10247))
- Add support for query profiler with concurrent aggregation ([#9248](https://github.com/opensearch-project/OpenSearch/pull/9248))
- Introduce ConcurrentQueryProfiler to profile query using concurrent segment search path and support concurrency during rewrite and create weight ([10352](https://github.com/opensearch-project/OpenSearch/pull/10352))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- [Streaming Indexing] Introduce new experimental server HTTP transport based on Netty 4 and Project Reactor (Reactor Netty) ([#9672](https://github.com/opensearch-project/OpenSearch/pull/9672))
- Add back half_float BKD based sort query optimization ([#11024](https://github.com/opensearch-project/OpenSearch/pull/11024))
- Request level coordinator slow logs ([#11246](https://github.com/opensearch-project/OpenSearch/pull/11246))
- Add template snippets support for field and target_field in KV ingest processor ([#10040](https://github.com/opensearch-project/OpenSearch/pull/10040))
- Allowing pipeline processors to access index mapping info by passing ingest service ref as part of the processor factory parameters ([#10307](https://github.com/opensearch-project/OpenSearch/pull/10307))

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
- Bump `org.apache.logging.log4j:log4j-core` from 2.20.0 to 2.21.1 ([#10858](https://github.com/opensearch-project/OpenSearch/pull/10858), [#11000](https://github.com/opensearch-project/OpenSearch/pull/11000))
- Bump `org.apache.logging.log4j:log4j-core` from 2.20.0 to 2.22.0 ([#10858](https://github.com/opensearch-project/OpenSearch/pull/10858), [#11000](https://github.com/opensearch-project/OpenSearch/pull/11000), [#11270](https://github.com/opensearch-project/OpenSearch/pull/11270))
- Bump `aws-actions/configure-aws-credentials` from 2 to 4 ([#10504](https://github.com/opensearch-project/OpenSearch/pull/10504))
- Bump `stefanzweifel/git-auto-commit-action` from 4 to 5 ([#11171](https://github.com/opensearch-project/OpenSearch/pull/11171))
- Bump `jackson` and `jackson_databind` from 2.15.2 to 2.16.0 ([#11273](https://github.com/opensearch-project/OpenSearch/pull/11273))
- Bump `netty` from 4.1.100.Final to 4.1.101.Final ([#11294](https://github.com/opensearch-project/OpenSearch/pull/11294))
- Bump `com.avast.gradle:gradle-docker-compose-plugin` from 0.16.12 to 0.17.5 ([#10163](https://github.com/opensearch-project/OpenSearch/pull/10163))
- Bump `com.squareup.okhttp3:okhttp` from 4.11.0 to 4.12.0 ([#10861](https://github.com/opensearch-project/OpenSearch/pull/10861))

### Changed
- Force merge with `only_expunge_deletes` honors max segment size ([#10036](https://github.com/opensearch-project/OpenSearch/pull/10036))
- Add the means to extract the contextual properties from HttpChannel, TcpCChannel and TrasportChannel without excessive typecasting ([#10562](https://github.com/opensearch-project/OpenSearch/pull/10562))
- Backport the PR #9107 for updating CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY setting to a dynamic setting ([#10606](https://github.com/opensearch-project/OpenSearch/pull/10606))
- Search pipelines now support asynchronous request and response processors to avoid blocking on a transport thread ([#10598](https://github.com/opensearch-project/OpenSearch/pull/10598))
- [Remote Store] Add Remote Store backpressure rejection stats to `_nodes/stats` ([#10524](https://github.com/opensearch-project/OpenSearch/pull/10524))
- [BUG] Fix java.lang.SecurityException in repository-gcs plugin ([#10642](https://github.com/opensearch-project/OpenSearch/pull/10642))
- Add telemetry tracer/metric enable flag and integ test. ([#10395](https://github.com/opensearch-project/OpenSearch/pull/10395))
- Performance improvement for Datetime field caching ([#4558](https://github.com/opensearch-project/OpenSearch/issues/4558))
- Add instrumentation for indexing in transport bulk action and transport shard bulk action. ([#10273](https://github.com/opensearch-project/OpenSearch/pull/10273))
- Disallow removing some metadata fields by remove ingest processor ([#10895](https://github.com/opensearch-project/OpenSearch/pull/10895))
- Performance improvement for MultiTerm Queries on Keyword fields ([#7057](https://github.com/opensearch-project/OpenSearch/issues/7057))
- Refactor common parts from the Rounding class into a separate 'round' package ([#11023](https://github.com/opensearch-project/OpenSearch/issues/11023))
- Disable concurrent aggs for Diversified Sampler and Sampler aggs ([#11087](https://github.com/opensearch-project/OpenSearch/issues/11087))
- Made leader/follower check timeout setting dynamic ([#10528](https://github.com/opensearch-project/OpenSearch/pull/10528))

### Deprecated

### Removed
- Remove deprecated classes for Rounding ([#10956](https://github.com/opensearch-project/OpenSearch/issues/10956))

### Fixed
- Fix class_cast_exception when passing int to _version and other metadata fields in ingest simulate API ([#10101](https://github.com/opensearch-project/OpenSearch/pull/10101))
- Fix passing wrong parameter when calling newConfigurationException() in DotExpanderProcessor ([#10737](https://github.com/opensearch-project/OpenSearch/pull/10737))
- Fix per request latency last phase not tracked ([#10934](https://github.com/opensearch-project/OpenSearch/pull/10934))
- Fix SuggestSearch.testSkipDuplicates by forcing refresh when indexing its test documents ([#11068](https://github.com/opensearch-project/OpenSearch/pull/11068))
- [BUG] Fix the thread context that is not properly cleared and messes up the traces ([#10873](https://github.com/opensearch-project/OpenSearch/pull/10873))
- Handle canMatchSearchAfter for frozen context scenario ([#11249](https://github.com/opensearch-project/OpenSearch/pull/11249))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
