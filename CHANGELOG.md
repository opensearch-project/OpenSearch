# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Introduce new dynamic cluster setting to control slice computation for concurrent segment search ([#9107](https://github.com/opensearch-project/OpenSearch/pull/9107))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679), [#10664](https://github.com/opensearch-project/OpenSearch/pull/10664))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- [Admission control] Add enhancements to FS stats to include read/write time, queue size and IO time ([#10541](https://github.com/opensearch-project/OpenSearch/pull/10541))
- [Admission control] Add Resource usage collector service and resource usage tracker  ([#9890](https://github.com/opensearch-project/OpenSearch/pull/9890))
- [Remote cluster state] Change file names for remote cluster state ([#10557](https://github.com/opensearch-project/OpenSearch/pull/10557))
- [Remote cluster state] Upload global metadata in cluster state to remote store([#10404](https://github.com/opensearch-project/OpenSearch/pull/10404))
- [Remote cluster state] Download functionality of global metadata from remote store ([#10535](https://github.com/opensearch-project/OpenSearch/pull/10535))
- [Remote cluster state] Restore global metadata from remote store when local state is lost after quorum loss ([#10404](https://github.com/opensearch-project/OpenSearch/pull/10404))
- [AdmissionControl] Added changes for AdmissionControl Interceptor and AdmissionControlService for RateLimiting ([#9286](https://github.com/opensearch-project/OpenSearch/pull/9286))
- GHA to verify checklist items completion in PR descriptions ([#10800](https://github.com/opensearch-project/OpenSearch/pull/10800))
- Allow to pass the list settings through environment variables (like [], ["a", "b", "c"], ...) ([#10625](https://github.com/opensearch-project/OpenSearch/pull/10625))
- [Remote cluster state] Restore cluster state version during remote state auto restore ([#10853](https://github.com/opensearch-project/OpenSearch/pull/10853))
- Add back half_float BKD based sort query optimization ([#11024](https://github.com/opensearch-project/OpenSearch/pull/11024))

### Dependencies
- Bump `log4j-core` from 2.18.0 to 2.19.0
- Bump `forbiddenapis` from 3.3 to 3.4
- Bump `avro` from 1.11.1 to 1.11.2
- Bump `woodstox-core` from 6.3.0 to 6.3.1
- Bump `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bump `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bump `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bump `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))
- Update Apache Lucene to 9.5.0-snapshot-d5cef1c ([#5570](https://github.com/opensearch-project/OpenSearch/pull/5570))
- Bump `maven-model` from 3.6.2 to 3.8.6 ([#5599](https://github.com/opensearch-project/OpenSearch/pull/5599))
- Bump `maxmind-db` from 2.1.0 to 3.0.0 ([#5601](https://github.com/opensearch-project/OpenSearch/pull/5601))
- Bump `wiremock-jre8-standalone` from 2.33.2 to 2.35.0
- Bump `gson` from 2.10 to 2.10.1
- Bump `com.google.code.gson:gson` from 2.10 to 2.10.1
- Bump `com.maxmind.geoip2:geoip2` from 4.0.0 to 4.0.1
- Bump `com.avast.gradle:gradle-docker-compose-plugin` from 0.16.11 to 0.16.12
- Bump `org.apache.commons:commons-configuration2` from 2.8.0 to 2.9.0
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.3.0
- Bump `io.opencensus:opencensus-api` from 0.18.0 to 0.31.1 ([#7291](https://github.com/opensearch-project/OpenSearch/pull/7291))
- OpenJDK Update (April 2023 Patch releases) ([#7344](https://github.com/opensearch-project/OpenSearch/pull/7344)
- Bump `com.google.http-client:google-http-client:1.43.2` from 1.42.0 to 1.43.2 ([7928](https://github.com/opensearch-project/OpenSearch/pull/7928)))
- Add Opentelemetry dependencies ([#7543](https://github.com/opensearch-project/OpenSearch/issues/7543))
- Bump `org.bouncycastle:bcprov-jdk15on` to `org.bouncycastle:bcprov-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump `org.bouncycastle:bcmail-jdk15on` to `org.bouncycastle:bcmail-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump `org.bouncycastle:bcpkix-jdk15on` to `org.bouncycastle:bcpkix-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump JNA version from 5.5 to 5.13 ([#9963](https://github.com/opensearch-project/OpenSearch/pull/9963))
- Bumps jetty version to 9.4.52.v20230823 to fix GMS-2023-1857 ([#9822](https://github.com/opensearch-project/OpenSearch/pull/9822))
- Bump `org.eclipse.jgit` from 6.5.0 to 6.7.0 ([#10147](https://github.com/opensearch-project/OpenSearch/pull/10147))
- Bump OpenTelemetry from 1.30.1 to 1.31.0 ([#10617](https://github.com/opensearch-project/OpenSearch/pull/10617))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))
- Add task completion count in search backpressure stats API ([#10028](https://github.com/opensearch-project/OpenSearch/pull/10028/))
- Performance improvement for Datetime field caching ([#4558](https://github.com/opensearch-project/OpenSearch/issues/4558))
- Deprecate CamelCase `PathHierarchy` tokenizer name in favor to lowercase `path_hierarchy` ([#10894](https://github.com/opensearch-project/OpenSearch/pull/10894))


### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))
- Remove custom Map, List and Set collection classes ([#6871](https://github.com/opensearch-project/OpenSearch/pull/6871))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Don't over-allocate in HeapBufferedAsyncEntityConsumer in order to consume the response ([#9993](https://github.com/opensearch-project/OpenSearch/pull/9993))

### Security

## [Unreleased 2.x]
### Added
- Per request phase latency ([#10351](https://github.com/opensearch-project/OpenSearch/issues/10351))
- Request level coordinator slow logs ([#10650](https://github.com/opensearch-project/OpenSearch/pull/10650))
- [Remote Store] Add repository stats for remote store([#10567](https://github.com/opensearch-project/OpenSearch/pull/10567))
- Add search query categorizer ([#10255](https://github.com/opensearch-project/OpenSearch/pull/10255))
- Introduce ConcurrentQueryProfiler to profile query using concurrent segment search path and support concurrency during rewrite and create weight ([10352](https://github.com/opensearch-project/OpenSearch/pull/10352))
- Update the indexRandom function to create more segments for concurrent search tests ([10247](https://github.com/opensearch-project/OpenSearch/pull/10247))
- [Remote cluster state] Make index and global metadata upload timeout dynamic cluster settings ([#10814](https://github.com/opensearch-project/OpenSearch/pull/10814))
- Add cluster state stats ([#10670](https://github.com/opensearch-project/OpenSearch/pull/10670))
- Adding slf4j license header to LoggerMessageFormat.java ([#11069](https://github.com/opensearch-project/OpenSearch/pull/11069))
- [Streaming Indexing] Introduce new experimental server HTTP transport based on Netty 4 and Project Reactor (Reactor Netty) ([#9672](https://github.com/opensearch-project/OpenSearch/pull/9672))
- Allowing pipeline processors to access index mapping info by passing ingest service ref as part of the processor factory parameters ([#10307](https://github.com/opensearch-project/OpenSearch/pull/10307))

### Dependencies
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.10.0 to 2.25.1 ([#10208](https://github.com/opensearch-project/OpenSearch/pull/10208), [#10298](https://github.com/opensearch-project/OpenSearch/pull/10298))
- Bump Lucene from 9.7.0 to 9.8.0 ([10276](https://github.com/opensearch-project/OpenSearch/pull/10276))
- Bump `com.netflix.nebula.ospackage-base` from 11.4.0 to 11.5.0 ([#10295](https://github.com/opensearch-project/OpenSearch/pull/10295))
- Bump `org.bouncycastle:bc-fips` from 1.0.2.3 to 1.0.2.4 ([#10297](https://github.com/opensearch-project/OpenSearch/pull/10297))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.0 to 3.9.1 ([#10506](https://github.com/opensearch-project/OpenSearch/pull/10506))
- Bump `de.thetaphi:forbiddenapis` from 3.5.1 to 3.6 ([#10508](https://github.com/opensearch-project/OpenSearch/pull/10508))
- Bump `commons-io:commons-io` from 2.13.0 to 2.14.0 ([#10294](https://github.com/opensearch-project/OpenSearch/pull/10294))
- Bump `org.codehaus.woodstox:stax2-api` from 4.2.1 to 4.2.2 ([#10639](https://github.com/opensearch-project/OpenSearch/pull/10639))
- Bump `com.google.http-client:google-http-client` from 1.43.2 to 1.43.3 ([#10635](https://github.com/opensearch-project/OpenSearch/pull/10635))
- Bump `com.squareup.okio:okio` from 3.5.0 to 3.6.0 ([#10637](https://github.com/opensearch-project/OpenSearch/pull/10637))
- Bump `org.apache.logging.log4j:log4j-core` from 2.20.0 to 2.21.1 ([#10858](https://github.com/opensearch-project/OpenSearch/pull/10858), [#11000](https://github.com/opensearch-project/OpenSearch/pull/11000))
- Bump `aws-actions/configure-aws-credentials` from 2 to 4 ([#10504](https://github.com/opensearch-project/OpenSearch/pull/10504))
- Bump `stefanzweifel/git-auto-commit-action` from 4 to 5 ([#11171](https://github.com/opensearch-project/OpenSearch/pull/11171))

### Changed
- Mute the query profile IT with concurrent execution ([#9840](https://github.com/opensearch-project/OpenSearch/pull/9840))
- Force merge with `only_expunge_deletes` honors max segment size ([#10036](https://github.com/opensearch-project/OpenSearch/pull/10036))
- Add the means to extract the contextual properties from HttpChannel, TcpCChannel and TrasportChannel without excessive typecasting ([#10562](https://github.com/opensearch-project/OpenSearch/pull/10562))
- Search pipelines now support asynchronous request and response processors to avoid blocking on a transport thread ([#10598](https://github.com/opensearch-project/OpenSearch/pull/10598))
- [Remote Store] Add Remote Store backpressure rejection stats to `_nodes/stats` ([#10524](https://github.com/opensearch-project/OpenSearch/pull/10524))
- [BUG] Fix java.lang.SecurityException in repository-gcs plugin ([#10642](https://github.com/opensearch-project/OpenSearch/pull/10642))
- Add telemetry tracer/metric enable flag and integ test. ([#10395](https://github.com/opensearch-project/OpenSearch/pull/10395))
- Add instrumentation for indexing in transport bulk action and transport shard bulk action. ([#10273](https://github.com/opensearch-project/OpenSearch/pull/10273))
- [BUG] Disable sort optimization for HALF_FLOAT ([#10999](https://github.com/opensearch-project/OpenSearch/pull/10999))
- Refactor common parts from the Rounding class into a separate 'round' package ([#11023](https://github.com/opensearch-project/OpenSearch/issues/11023))
- Performance improvement for MultiTerm Queries on Keyword fields ([#7057](https://github.com/opensearch-project/OpenSearch/issues/7057))
- Disable concurrent aggs for Diversified Sampler and Sampler aggs ([#11087](https://github.com/opensearch-project/OpenSearch/issues/11087))
- Made leader/follower check timeout setting dynamic ([#10528](https://github.com/opensearch-project/OpenSearch/pull/10528))
- Use iterative approach to evaluate Regex.simpleMatch ([#11060](https://github.com/opensearch-project/OpenSearch/pull/11060))

### Deprecated

### Removed
- Remove deprecated classes for Rounding ([#10956](https://github.com/opensearch-project/OpenSearch/issues/10956))

### Fixed
- Fix failure in dissect ingest processor parsing empty brackets ([#9225](https://github.com/opensearch-project/OpenSearch/pull/9255))
- Fix class_cast_exception when passing int to _version and other metadata fields in ingest simulate API ([#10101](https://github.com/opensearch-project/OpenSearch/pull/10101))
- Fix Segment Replication ShardLockObtainFailedException bug during index corruption ([10370](https://github.com/opensearch-project/OpenSearch/pull/10370))
- Fix some test methods in SimulatePipelineRequestParsingTests never run and fix test failure ([#10496](https://github.com/opensearch-project/OpenSearch/pull/10496))
- Fix passing wrong parameter when calling newConfigurationException() in DotExpanderProcessor ([#10737](https://github.com/opensearch-project/OpenSearch/pull/10737))
- Fix SuggestSearch.testSkipDuplicates by forceing refresh when indexing its test documents ([#11068](https://github.com/opensearch-project/OpenSearch/pull/11068))
- Adding version condition while adding geoshape doc values to the index, to ensure backward compatibility.([#11095](https://github.com/opensearch-project/OpenSearch/pull/11095))
- Fix per request latency last phase not tracked ([#10934](https://github.com/opensearch-project/OpenSearch/pull/10934))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
