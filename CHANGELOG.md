# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add support for ppc64le architecture ([#5459](https://github.com/opensearch-project/OpenSearch/pull/5459))

### Dependencies
- Bumps `log4j-core` from 2.18.0 to 2.19.0
- Bumps `reactor-netty-http` from 1.0.18 to 1.0.23
- Bumps `jettison` from 1.5.0 to 1.5.3
- Bumps `forbiddenapis` from 3.3 to 3.4
- Bumps `avro` from 1.11.0 to 1.11.1
- Bumps `woodstox-core` from 6.3.0 to 6.3.1
- Bumps `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bumps `azure-storage-common` from 12.18.0 to 12.18.1 ([#4164](https://github.com/opensearch-project/OpenSearch/pull/4664))
- Bumps `org.gradle.test-retry` from 1.4.0 to 1.4.1 ([#4411](https://github.com/opensearch-project/OpenSearch/pull/4411))
- Bumps `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bumps `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bumps `com.diffplug.spotless` from 6.10.0 to 6.11.0 ([#4547](https://github.com/opensearch-project/OpenSearch/pull/4547))
- Bumps `reactor-core` from 3.4.23 to 3.5.1 ([#5604](https://github.com/opensearch-project/OpenSearch/pull/5604))
- Bumps `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Bumps `spock-core` from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#5315](https://github.com/opensearch-project/OpenSearch/pull/5315))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))
- Update Apache Lucene to 9.5.0-snapshot-d5cef1c ([#5570](https://github.com/opensearch-project/OpenSearch/pull/5570))
- Bump antlr4 from 4.9.3 to 4.11.1 ([#4546](https://github.com/opensearch-project/OpenSearch/pull/4546))
- Bumps `maven-model` from 3.6.2 to 3.8.6 ([#5599](https://github.com/opensearch-project/OpenSearch/pull/5599))
- Bumps `maxmind-db` from 2.1.0 to 3.0.0 ([#5601](https://github.com/opensearch-project/OpenSearch/pull/5601))
- Bumps `protobuf-java` from 3.21.11 to 3.21.12 ([#5603](https://github.com/opensearch-project/OpenSearch/pull/5603))
- Bumps `azure-core-http-netty` from 1.12.7 to 1.12.8
- Bumps `wiremock-jre8-standalone` from 2.33.2 to 2.35.0
- Bumps `gson` from 2.10 to 2.10.1
- Bumps `json-schema-validator` from 1.0.73 to 1.0.76
- Bumps `jna` from 5.11.0 to 5.13.0
- Bumps `joni` from 2.1.44 to 2.1.45

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Changed http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Require MediaType in Strings.toString API ([#6009](https://github.com/opensearch-project/OpenSearch/pull/6009))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))

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

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fixed compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Support OpenSSL Provider with default Netty allocator ([#5460](https://github.com/opensearch-project/OpenSearch/pull/5460))

### Security

## [Unreleased 2.x]
### Added
- Adding index create block when all nodes have breached high disk watermark ([#5852](https://github.com/opensearch-project/OpenSearch/pull/5852))
- Added cluster manager throttling stats in nodes/stats API ([#5790](https://github.com/opensearch-project/OpenSearch/pull/5790))
- Added support for feature flags in opensearch.yml ([#4959](https://github.com/opensearch-project/OpenSearch/pull/4959))
- Add query for initialized extensions ([#5658](https://github.com/opensearch-project/OpenSearch/pull/5658))
- Add update-index-settings allowlist for searchable snapshot ([#5907](https://github.com/opensearch-project/OpenSearch/pull/5907))
- Replace latches with CompletableFutures for extensions ([#5646](https://github.com/opensearch-project/OpenSearch/pull/5646))
- Add GeoTile and GeoHash Grid aggregations on GeoShapes. ([#5589](https://github.com/opensearch-project/OpenSearch/pull/5589))
- Add support to disallow search request with preference parameter with strict weighted shard routing([#5874](https://github.com/opensearch-project/OpenSearch/pull/5874))
- Added support to apply index create block ([#4603](https://github.com/opensearch-project/OpenSearch/issues/4603))
- Adds support for minimum compatible version for extensions ([#6003](https://github.com/opensearch-project/OpenSearch/pull/6003))
- Add a guardrail to limit maximum number of shard on the cluster ([#6143](https://github.com/opensearch-project/OpenSearch/pull/6143))
- Add cancellation of in-flight SearchTasks based on resource consumption ([#5606](https://github.com/opensearch-project/OpenSearch/pull/5605))

### Dependencies
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))
- Bumps `reactor-netty` from 1.1.1 to 1.1.2 ([#5878](https://github.com/opensearch-project/OpenSearch/pull/5878))
- OpenJDK Update (January 2023 Patch releases) ([#6074](https://github.com/opensearch-project/OpenSearch/pull/6074))
- Bumps `Mockito` from 4.7.0 to 5.1.0, `ByteBuddy` from 1.12.18 to 1.12.22 ([#6076](https://github.com/opensearch-project/OpenSearch/pull/6076))
- Bumps `joda` from 2.10.13 to 2.12.2 ([#6083](https://github.com/opensearch-project/OpenSearch/pull/6083))
- Upgrade to Lucene 9.5.0 ([#5878](https://github.com/opensearch-project/OpenSearch/pull/6078))
- Bumps `Jackson` from 2.14.1 to 2.14.2 ([#6129](https://github.com/opensearch-project/OpenSearch/pull/6129))
- Bumps `Netty` from 4.1.86.Final to 4.1.87.Final ([#6130](https://github.com/opensearch-project/OpenSearch/pull/6130))

### Changed
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- [Refactor] Use local opensearch.common.SetOnce instead of lucene's utility class ([#5947](https://github.com/opensearch-project/OpenSearch/pull/5947))
- Cluster health call to throw decommissioned exception for local decommissioned node([#6008](https://github.com/opensearch-project/OpenSearch/pull/6008))
- [Refactor] core.common to new opensearch-common library ([#5976](https://github.com/opensearch-project/OpenSearch/pull/5976))

### Deprecated

### Removed

### Fixed
- [Segment Replication] Fix for peer recovery ([#5344](https://github.com/opensearch-project/OpenSearch/pull/5344))
- Fix weighted shard routing state across search requests([#6004](https://github.com/opensearch-project/OpenSearch/pull/6004))
- [Segment Replication] Fix bug where inaccurate sequence numbers are sent during replication ([#6122](https://github.com/opensearch-project/OpenSearch/pull/6122))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.5...2.x
