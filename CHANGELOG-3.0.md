# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679), [#10664](https://github.com/opensearch-project/OpenSearch/pull/10664))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- GHA to verify checklist items completion in PR descriptions ([#10800](https://github.com/opensearch-project/OpenSearch/pull/10800))
- Allow to pass the list settings through environment variables (like [], ["a", "b", "c"], ...) ([#10625](https://github.com/opensearch-project/OpenSearch/pull/10625))
- Views, simplify data access and manipulation by providing a virtual layer over one or more indices ([#11957](https://github.com/opensearch-project/OpenSearch/pull/11957))

### Dependencies
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
- Bump Jackson version from 2.16.1 to 2.16.2 ([#12611](https://github.com/opensearch-project/OpenSearch/pull/12611))
- Bump `aws-sdk-java` from 2.20.55 to 2.20.86 ([#12251](https://github.com/opensearch-project/OpenSearch/pull/12251))

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
- Breaking change: Do not request "search_pipelines" metrics by default in NodesInfoRequest ([#12497](https://github.com/opensearch-project/OpenSearch/pull/12497))

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

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
