# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))

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
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0
- Bump `org.apache.commons:commons-configuration2` from 2.8.0 to 2.9.0
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.3.0
- Bump `io.opencensus:opencensus-api` from 0.18.0 to 0.31.1 ([#7291](https://github.com/opensearch-project/OpenSearch/pull/7291))
- OpenJDK Update (April 2023 Patch releases) ([#7344](https://github.com/opensearch-project/OpenSearch/pull/7344)
- Bump `com.google.http-client:google-http-client:1.43.2` from 1.42.0 to 1.43.2 ([7928](https://github.com/opensearch-project/OpenSearch/pull/7928)))
- Add Opentelemetry dependencies ([#7543](https://github.com/opensearch-project/OpenSearch/issues/7543))
- Bump `org.bouncycastle:bcprov-jdk15on` to `org.bouncycastle:bcprov-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump `org.bouncycastle:bcmail-jdk15on` to `org.bouncycastle:bcmail-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump `org.bouncycastle:bcpkix-jdk15on` to `org.bouncycastle:bcpkix-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Remote Segment Store Repository setting moved from `index.remote_store.repository` to `index.remote_store.segment.repository` and `cluster.remote_store.repository` to `cluster.remote_store.segment.repository` respectively for Index and Cluster level settings ([#8719](https://github.com/opensearch-project/OpenSearch/pull/8719))
- [Remote Store] Add support to restore only unassigned shards of an index ([#8792](https://github.com/opensearch-project/OpenSearch/pull/8792))
- Replace the deprecated IndexReader APIs with new storedFields() & termVectors() ([#7792](https://github.com/opensearch-project/OpenSearch/pull/7792))
- [Remote Store] Restrict user override for remote store index level settings ([#8812](https://github.com/opensearch-project/OpenSearch/pull/8812))

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
- Remove provision to create Remote Indices without Remote Translog Store ([#8719](https://github.com/opensearch-project/OpenSearch/pull/8719))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))

### Security

## [Unreleased 2.x]
### Added
- Add server version as REST response header [#6583](https://github.com/opensearch-project/OpenSearch/issues/6583)
- Start replication checkpointTimers on primary before segments upload to remote store. ([#8221]()https://github.com/opensearch-project/OpenSearch/pull/8221)
- [distribution/archives] [Linux] [x64] Provide the variant of the distributions bundled with JRE ([#8195]()https://github.com/opensearch-project/OpenSearch/pull/8195)
- Add configuration for file cache size to max remote data ratio to prevent oversubscription of file cache ([#8606](https://github.com/opensearch-project/OpenSearch/pull/8606))
- Disallow compression level to be set for default and best_compression index codecs ([#8737]()https://github.com/opensearch-project/OpenSearch/pull/8737)

### Dependencies
- Bump `org.apache.logging.log4j:log4j-core` from 2.17.1 to 2.20.0 ([#8307](https://github.com/opensearch-project/OpenSearch/pull/8307))
- Bump `io.grpc:grpc-context` from 1.46.0 to 1.56.1 ([#8726](https://github.com/opensearch-project/OpenSearch/pull/8726))
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.5 to 12.1.6 ([#8724](https://github.com/opensearch-project/OpenSearch/pull/8724))
- Bump `commons-codec:commons-codec` from 1.15 to 1.16.0 ([#8725](https://github.com/opensearch-project/OpenSearch/pull/8725))
- Bump `org.apache.zookeeper:zookeeper` from 3.8.1 to 3.8.2 ([#8844](https://github.com/opensearch-project/OpenSearch/pull/8844))
- Bump `org.gradle.test-retry` from 1.5.3 to 1.5.4 ([#8842](https://github.com/opensearch-project/OpenSearch/pull/8842))
- Bump `com.netflix.nebula.ospackage-base` from 11.3.0 to 11.4.0 ([#8838](https://github.com/opensearch-project/OpenSearch/pull/8838))
- Bump `com.google.http-client:google-http-client-gson` from 1.43.2 to 1.43.3 ([#8840](https://github.com/opensearch-project/OpenSearch/pull/8840))
- OpenJDK Update (July 2023 Patch releases) ([#8868](https://github.com/opensearch-project/OpenSearch/pull/8868)
- Bump `hadoop` libraries from 3.3.4 to 3.3.6 ([#6995](https://github.com/opensearch-project/OpenSearch/pull/6995))
- Bump `com.gradle.enterprise` from 3.13.3 to 3.14.1 ([#8996](https://github.com/opensearch-project/OpenSearch/pull/8996))
- Bump `org.apache.commons:commons-lang3` from 3.12.0 to 3.13.0 ([#8995](https://github.com/opensearch-project/OpenSearch/pull/8995))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.0 to 2.21.1 ([#8999](https://github.com/opensearch-project/OpenSearch/pull/8999))
- Bump `com.maxmind.geoip2:geoip2` from 4.0.1 to 4.1.0 ([#8998](https://github.com/opensearch-project/OpenSearch/pull/8998))
- Bump `org.apache.commons:commons-lang3` from 3.12.0 to 3.13.0 in /plugins/repository-hdfs ([#8997](https://github.com/opensearch-project/OpenSearch/pull/8997))
- Bump `netty` from 4.1.94.Final to 4.1.96.Final ([#9030](https://github.com/opensearch-project/OpenSearch/pull/9030))

### Changed
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))
- Make Span exporter configurable ([#8620](https://github.com/opensearch-project/OpenSearch/issues/8620))
- Change InternalSignificantTerms to sum shard-level superset counts only in final reduce ([#8735](https://github.com/opensearch-project/OpenSearch/pull/8735))
- Exclude 'benchmarks' from codecov report ([#8805](https://github.com/opensearch-project/OpenSearch/pull/8805))
- [Refactor] MediaTypeParser to MediaTypeParserRegistry ([#8636](https://github.com/opensearch-project/OpenSearch/pull/8636))
- Create separate SourceLookup instance per segment slice in SignificantTextAggregatorFactory ([#8807](https://github.com/opensearch-project/OpenSearch/pull/8807))
- Add support for aggregation profiler with concurrent aggregation ([#8801](https://github.com/opensearch-project/OpenSearch/pull/8801))
- [Remove] Deprecated Fractional ByteSizeValue support #9005 ([#9005](https://github.com/opensearch-project/OpenSearch/pull/9005))

### Deprecated

### Removed

### Fixed
- Fix flaky ResourceAwareTasksTests.testBasicTaskResourceTracking test ([#8993](https://github.com/opensearch-project/OpenSearch/pull/8993))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.10...2.x
