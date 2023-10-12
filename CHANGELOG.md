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
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679))
- Implement Visitor Design pattern in QueryBuilder to enable the capability to traverse through the complex QueryBuilder tree. ([#10110](https://github.com/opensearch-project/OpenSearch/pull/10110))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- Configurable merge policy for index with an option to choose from LogByteSize and Tiered merge policy ([#9992](https://github.com/opensearch-project/OpenSearch/pull/9992))

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

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))
- Add task completion count in search backpressure stats API ([#10028](https://github.com/opensearch-project/OpenSearch/pull/10028/))
- Performance improvement for Datetime field caching ([#4558](https://github.com/opensearch-project/OpenSearch/issues/4558))


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

### Dependencies

### Changed

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
