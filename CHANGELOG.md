# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679), [#10664](https://github.com/opensearch-project/OpenSearch/pull/10664))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- GHA to verify checklist items completion in PR descriptions ([#10800](https://github.com/opensearch-project/OpenSearch/pull/10800))
- Allow to pass the list settings through environment variables (like [], ["a", "b", "c"], ...) ([#10625](https://github.com/opensearch-project/OpenSearch/pull/10625))
- [S3 Repository] Add setting to control connection count for sync client ([#12028](https://github.com/opensearch-project/OpenSearch/pull/12028))
- Views, simplify data access and manipulation by providing a virtual layer over one or more indices ([#11957](https://github.com/opensearch-project/OpenSearch/pull/11957))
- Add Remote Store Migration Experimental flag and allow mixed mode clusters under same ([#11986](https://github.com/opensearch-project/OpenSearch/pull/11986))
- [Admission Control] Integrate IO Usage Tracker to the Resource Usage Collector Service and Emit IO Usage Stats ([#11880](https://github.com/opensearch-project/OpenSearch/pull/11880))

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
- Bump `org.eclipse.jgit` from 6.5.0 to 6.7.0 ([#10147](https://github.com/opensearch-project/OpenSearch/pull/10147))
- Bump OpenTelemetry from 1.30.1 to 1.31.0 ([#10617](https://github.com/opensearch-project/OpenSearch/pull/10617))
- Bump OpenTelemetry from 1.31.0 to 1.32.0 and OpenTelemetry Semconv from 1.21.0-alpha to 1.23.1-alpha ([#11305](https://github.com/opensearch-project/OpenSearch/pull/11305))
- Bump `org.bouncycastle:bcprov-jdk15to18` to `org.bouncycastle:bcprov-jdk18on` version 1.77 ([#12317](https://github.com/opensearch-project/OpenSearch/pull/12317))
- Bump `org.bouncycastle:bcmail-jdk15to18` to `org.bouncycastle:bcmail-jdk18on` version 1.77 ([#12317](https://github.com/opensearch-project/OpenSearch/pull/12317))
- Bump `org.bouncycastle:bcpkix-jdk15to18` to `org.bouncycastle:bcpkix-jdk18on` version 1.77 ([#12317](https://github.com/opensearch-project/OpenSearch/pull/12317))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))
- Add task completion count in search backpressure stats API ([#10028](https://github.com/opensearch-project/OpenSearch/pull/10028/))
- Deprecate CamelCase `PathHierarchy` tokenizer name in favor to lowercase `path_hierarchy` ([#10894](https://github.com/opensearch-project/OpenSearch/pull/10894))
- Switched to more reliable OpenSearch Lucene snapshot location([#11728](https://github.com/opensearch-project/OpenSearch/pull/11728))

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
- Update supported version for max_shard_size parameter in Shrink API ([#11439](https://github.com/opensearch-project/OpenSearch/pull/11439))
- Fix typo in API annotation check message ([11836](https://github.com/opensearch-project/OpenSearch/pull/11836))
- Update supported version for must_exist parameter in update aliases API ([#11872](https://github.com/opensearch-project/OpenSearch/pull/11872))
- [Bug] Check phase name before SearchRequestOperationsListener onPhaseStart ([#12035](https://github.com/opensearch-project/OpenSearch/pull/12035))
- Fix Span operation names generated from RestActions ([#12005](https://github.com/opensearch-project/OpenSearch/pull/12005))
- Fix error in RemoteSegmentStoreDirectory when debug logging is enabled ([#12328](https://github.com/opensearch-project/OpenSearch/pull/12328))

### Security

## [Unreleased 2.x]
### Added
- [Tiered caching] Introducing cache plugins and exposing Ehcache as one of the pluggable disk cache option ([#11874](https://github.com/opensearch-project/OpenSearch/pull/11874))
- Add support for dependencies in plugin descriptor properties with semver range ([#11441](https://github.com/opensearch-project/OpenSearch/pull/11441))
- Add community_id ingest processor ([#12121](https://github.com/opensearch-project/OpenSearch/pull/12121))
- Introduce query level setting `index.query.max_nested_depth` limiting nested queries ([#3268](https://github.com/opensearch-project/OpenSearch/issues/3268)
- Add toString methods to MultiSearchRequest, MultiGetRequest and CreateIndexRequest ([#12163](https://github.com/opensearch-project/OpenSearch/pull/12163))
- Support for returning scores in matched queries ([#11626](https://github.com/opensearch-project/OpenSearch/pull/11626))
- Add shard id property to SearchLookup for use in field types provided by plugins ([#1063](https://github.com/opensearch-project/OpenSearch/pull/1063))
- Add kuromoji_completion analyzer and filter ([#4835](https://github.com/opensearch-project/OpenSearch/issues/4835))

### Dependencies
- Bump `peter-evans/find-comment` from 2 to 3 ([#12288](https://github.com/opensearch-project/OpenSearch/pull/12288))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.25.1 to 2.33.0 ([#12289](https://github.com/opensearch-project/OpenSearch/pull/12289))
- Bump `com.squareup.okio:okio` from 3.7.0 to 3.8.0 ([#12290](https://github.com/opensearch-project/OpenSearch/pull/12290))
- Bump `gradle/wrapper-validation-action` from 1 to 2 ([#12367](https://github.com/opensearch-project/OpenSearch/pull/12367))
- Bump `netty` from 4.1.106.Final to 4.1.107.Final ([#12372](https://github.com/opensearch-project/OpenSearch/pull/12372))
- Bump `opentelemetry` from 1.34.1 to 1.35.0 ([#12388](https://github.com/opensearch-project/OpenSearch/pull/12388))
- Bump Apache Lucene from 9.9.2 to 9.10.0 ([#12392](https://github.com/opensearch-project/OpenSearch/pull/12392))
- Bump `org.apache.logging.log4j:log4j-core` from 2.22.1 to 2.23.1 ([#12464](https://github.com/opensearch-project/OpenSearch/pull/12464), [#12587](https://github.com/opensearch-project/OpenSearch/pull/12587))
- Bump `antlr4` from 4.11.1 to 4.13.1 ([#12445](https://github.com/opensearch-project/OpenSearch/pull/12445))
- Bump `com.netflix.nebula.ospackage-base` from 11.8.0 to 11.8.1 ([#12461](https://github.com/opensearch-project/OpenSearch/pull/12461))
- Bump `peter-evans/create-or-update-comment` from 3 to 4 ([#12462](https://github.com/opensearch-project/OpenSearch/pull/12462))
- Bump `lycheeverse/lychee-action` from 1.9.1 to 1.9.3 ([#12521](https://github.com/opensearch-project/OpenSearch/pull/12521))
- Bump `com.azure:azure-core` from 1.39.0 to 1.47.0 ([#12520](https://github.com/opensearch-project/OpenSearch/pull/12520))
- Bump `ch.qos.logback:logback-core` from 1.2.13 to 1.5.3 ([#12519](https://github.com/opensearch-project/OpenSearch/pull/12519))
- Bump `codecov/codecov-action` from 3 to 4 ([#12585](https://github.com/opensearch-project/OpenSearch/pull/12585))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.1 to 3.9.2 ([#12580](https://github.com/opensearch-project/OpenSearch/pull/12580))
- Bump `org.codehaus.woodstox:stax2-api` from 4.2.1 to 4.2.2 ([#12579](https://github.com/opensearch-project/OpenSearch/pull/12579))

### Changed
- Allow composite aggregation to run under a parent filter aggregation ([#11499](https://github.com/opensearch-project/OpenSearch/pull/11499))

### Deprecated

### Removed

### Fixed
- Fix for deserilization bug in weighted round-robin metadata ([#11679](https://github.com/opensearch-project/OpenSearch/pull/11679))
- [Revert] [Bug] Check phase name before SearchRequestOperationsListener onPhaseStart ([#12035](https://github.com/opensearch-project/OpenSearch/pull/12035))
- Add support of special WrappingSearchAsyncActionPhase so the onPhaseStart() will always be followed by onPhaseEnd() within AbstractSearchAsyncAction ([#12293](https://github.com/opensearch-project/OpenSearch/pull/12293))
- Add a system property to configure YamlParser codepoint limits ([#12298](https://github.com/opensearch-project/OpenSearch/pull/12298))
- Prevent read beyond slice boundary in ByteArrayIndexInput ([#10481](https://github.com/opensearch-project/OpenSearch/issues/10481))
- Fix the "highlight.max_analyzer_offset" request parameter with "plain" highlighter ([#10919](https://github.com/opensearch-project/OpenSearch/pull/10919))
- Warn about deprecated and ignored index.mapper.dynamic index setting ([#11193](https://github.com/opensearch-project/OpenSearch/pull/11193))
- Fix `terms` query on `float` field when `doc_values` are turned off by reverting back to `FloatPoint` from `FloatField` ([#12499](https://github.com/opensearch-project/OpenSearch/pull/12499))
- Fix get task API does not refresh resource stats ([#11531](https://github.com/opensearch-project/OpenSearch/pull/11531))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
