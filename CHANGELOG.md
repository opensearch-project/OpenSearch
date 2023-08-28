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
- Add coordinator level stats for search latency ([#8386](https://github.com/opensearch-project/OpenSearch/issues/8386))
- Add metrics for thread_pool task wait time ([#9681](https://github.com/opensearch-project/OpenSearch/pull/9681))
- Async blob read support for S3 plugin ([#9694](https://github.com/opensearch-project/OpenSearch/pull/9694))
- [Telemetry-Otel] Added support for OtlpGrpcSpanExporter exporter ([#9666](https://github.com/opensearch-project/OpenSearch/pull/9666))
- Async blob read support for encrypted containers ([#10131](https://github.com/opensearch-project/OpenSearch/pull/10131))
- Add capability to restrict async durability mode for remote indexes ([#10189](https://github.com/opensearch-project/OpenSearch/pull/10189))
- Add Doc Status Counter for Indexing Engine ([#4562](https://github.com/opensearch-project/OpenSearch/issues/4562))
- Add unreferenced file cleanup count to merge stats ([#10204](https://github.com/opensearch-project/OpenSearch/pull/10204))
- [Remote Store] Add support to restrict creation & deletion if system repository and mutation of immutable settings of system repository ([#9839](https://github.com/opensearch-project/OpenSearch/pull/9839))

### Dependencies
- Bump `peter-evans/create-or-update-comment` from 2 to 3 ([#9575](https://github.com/opensearch-project/OpenSearch/pull/9575))
- Bump `actions/checkout` from 2 to 4 ([#9968](https://github.com/opensearch-project/OpenSearch/pull/9968))
- Bump OpenTelemetry from 1.26.0 to 1.30.1 ([#9950](https://github.com/opensearch-project/OpenSearch/pull/9950))
- Bump `org.apache.commons:commons-compress` from 1.23.0 to 1.24.0 ([#9973, #9972](https://github.com/opensearch-project/OpenSearch/pull/9973, https://github.com/opensearch-project/OpenSearch/pull/9972))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.1 to 2.23.0 ([#9971](https://github.com/opensearch-project/OpenSearch/pull/9971))
- Bump `mockito` from 5.4.0 to 5.5.0 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `bytebuddy` from 1.14.3 to 1.14.7 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `com.zaxxer:SparseBitSet` from 1.2 to 1.3 ([#10098](https://github.com/opensearch-project/OpenSearch/pull/10098))
- Bump `tibdex/github-app-token` from 1.5.0 to 2.1.0 ([#10125](https://github.com/opensearch-project/OpenSearch/pull/10125))
- Bump `org.wiremock:wiremock-standalone` from 2.35.0 to 3.1.0 ([#9752](https://github.com/opensearch-project/OpenSearch/pull/9752))
- Bump `com.google.http-client:google-http-client-jackson2` from 1.43.2 to 1.43.3 ([#10126](https://github.com/opensearch-project/OpenSearch/pull/10126))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.3 to 1.1.10.4 ([#10206](https://github.com/opensearch-project/OpenSearch/pull/10206))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.10.0 to 2.25.0 ([#10208](https://github.com/opensearch-project/OpenSearch/pull/10208))
- Bump `codecov/codecov-action` from 2 to 3 ([#10209](https://github.com/opensearch-project/OpenSearch/pull/10209))
- Bump `org.bouncycastle:bcpkix-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`
- Bump `org.bouncycastle:bcprov-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`
- Bump `org.bouncycastle:bcmail-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`
- Bump Lucene from 9.7.0 to 9.8.0 ([10276](https://github.com/opensearch-project/OpenSearch/pull/10276))
- Bump `com.netflix.nebula.ospackage-base` from 11.4.0 to 11.5.0 ([#10295](https://github.com/opensearch-project/OpenSearch/pull/10295))
- Bump asm from 9.5 to 9.6 ([#10302](https://github.com/opensearch-project/OpenSearch/pull/10302))
- Bump netty from 4.1.97.Final to 4.1.99.Final ([#10303](https://github.com/opensearch-project/OpenSearch/pull/10303))
- Bump `peter-evans/create-pull-request` from 3 to 5 ([#10301](https://github.com/opensearch-project/OpenSearch/pull/10301))

### Changed
- Add instrumentation in rest and network layer. ([#9415](https://github.com/opensearch-project/OpenSearch/pull/9415))
- Allow parameterization of tests with OpenSearchIntegTestCase.SuiteScopeTestCase annotation ([#9916](https://github.com/opensearch-project/OpenSearch/pull/9916))
- Mute the query profile IT with concurrent execution ([#9840](https://github.com/opensearch-project/OpenSearch/pull/9840))
- Force merge with `only_expunge_deletes` honors max segment size ([#10036](https://github.com/opensearch-project/OpenSearch/pull/10036))
- Add instrumentation in transport service. ([#10042](https://github.com/opensearch-project/OpenSearch/pull/10042))
- [Tracing Framework] Add support for SpanKind. ([#10122](https://github.com/opensearch-project/OpenSearch/pull/10122))
- Pass parent filter to inner query in nested query ([#10246](https://github.com/opensearch-project/OpenSearch/pull/10246))
- Disable concurrent segment search when terminate_after is used ([#10200](https://github.com/opensearch-project/OpenSearch/pull/10200))

### Deprecated

### Removed
- Remove spurious SGID bit on directories ([#9447](https://github.com/opensearch-project/OpenSearch/pull/9447))

### Fixed
- Fix ignore_missing parameter has no effect when using template snippet in rename ingest processor ([#9725](https://github.com/opensearch-project/OpenSearch/pull/9725))
- Fix broken backward compatibility from 2.7 for IndexSorted field indices ([#10045](https://github.com/opensearch-project/OpenSearch/pull/10045))
- Fix concurrent search NPE when track_total_hits, terminate_after and size=0 are used ([#10082](https://github.com/opensearch-project/OpenSearch/pull/10082))
- Fix remove ingest processor handing ignore_missing parameter not correctly ([10089](https://github.com/opensearch-project/OpenSearch/pull/10089))
- Fix circular dependency in Settings initialization ([10194](https://github.com/opensearch-project/OpenSearch/pull/10194))
- Fix registration and initialization of multiple extensions ([10256](https://github.com/opensearch-project/OpenSearch/pull/10256))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.11...2.x
