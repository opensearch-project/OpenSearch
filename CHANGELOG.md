
# CHANGELOG
## [Unreleased]
### Dependencies
- Bumps `azure-core-http-netty` from 1.12.7 to 1.12.8


All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Add support of default replica count cluster setting ([#5610](https://github.com/opensearch-project/OpenSearch/pull/5610))
- Hardened token permissions in GitHub workflows ([#4587](https://github.com/opensearch-project/OpenSearch/pull/4587))
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Add project health badges to the README.md ([#4843](https://github.com/opensearch-project/OpenSearch/pull/4843))
- [Test] Add IAE test for deprecated edgeNGram analyzer name ([#5040](https://github.com/opensearch-project/OpenSearch/pull/5040))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add feature flag for extensions ([#5211](https://github.com/opensearch-project/OpenSearch/pull/5211))
- Support to fail open requests on search shard failures with weighted traffic routing ([#5072](https://github.com/opensearch-project/OpenSearch/pull/5072))
- Added jackson dependency to server ([#5366] (https://github.com/opensearch-project/OpenSearch/pull/5366))
- Adding support to register settings dynamically ([#5495](https://github.com/opensearch-project/OpenSearch/pull/5495))
- Added experimental support for extensions ([#5347](https://github.com/opensearch-project/OpenSearch/pull/5347)), ([#5518](https://github.com/opensearch-project/OpenSearch/pull/5518), ([#5597](https://github.com/opensearch-project/OpenSearch/pull/5597)), ([#5615](https://github.com/opensearch-project/OpenSearch/pull/5615)))
- Add CI bundle pattern to distribution download ([#5348](https://github.com/opensearch-project/OpenSearch/pull/5348))
- Add support for ppc64le architecture ([#5459](https://github.com/opensearch-project/OpenSearch/pull/5459))
- Support versioning for Weighted routing apis([#5255](https://github.com/opensearch-project/OpenSearch/pull/5255))
- Added @gbbafna as an OpenSearch maintainer ([#5668](https://github.com/opensearch-project/OpenSearch/pull/5668))
- Add support for discovered cluster manager and remove local weights ([#5680](https://github.com/opensearch-project/OpenSearch/pull/5680))
- Added support for feature flags in opensearch.yml ([#4959](https://github.com/opensearch-project/OpenSearch/pull/4959))
- Add query for initialized extensions ([#5658](https://github.com/opensearch-project/OpenSearch/pull/5658))
- Revert 'Added jackson dependency to server' and change extension reading ([#5768](https://github.com/opensearch-project/OpenSearch/pull/5768))

### Dependencies
- Bumps `log4j-core` from 2.18.0 to 2.19.0
- Bumps `reactor-netty-http` from 1.0.18 to 1.0.23
- Bumps `jettison` from 1.5.0 to 1.5.1
- Bumps `forbiddenapis` from 3.3 to 3.4
- Bumps `gson` from 2.9.0 to 2.10
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
- Bumps `commons-compress` from 1.21 to 1.22
- Bumps `jcodings` from 1.0.57 to 1.0.58 ([#5233](https://github.com/opensearch-project/OpenSearch/pull/5233))
- Bumps `google-http-client-jackson2` from 1.35.0 to 1.42.3 ([#5234](https://github.com/opensearch-project/OpenSearch/pull/5234))
- Bumps `azure-core` from 1.33.0 to 1.34.0 ([#5235](https://github.com/opensearch-project/OpenSearch/pull/5235))
- Bumps `azure-core-http-netty` from 1.12.4 to 1.12.7 ([#5235](https://github.com/opensearch-project/OpenSearch/pull/5235))
- Bumps `spock-core` from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#5315](https://github.com/opensearch-project/OpenSearch/pull/5315))
- Bumps `json-schema-validator` from 1.0.69 to 1.0.73 ([#5316](https://github.com/opensearch-project/OpenSearch/pull/5316))
- Bumps `proto-google-common-protos` from 2.8.0 to 2.10.0 ([#5318](https://github.com/opensearch-project/OpenSearch/pull/5318))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))
- Update Apache Lucene to 9.5.0-snapshot-d5cef1c ([#5570](https://github.com/opensearch-project/OpenSearch/pull/5570))
- Bump antlr4 from 4.9.3 to 4.11.1 ([#4546](https://github.com/opensearch-project/OpenSearch/pull/4546))
- Bumps `maven-model` from 3.6.2 to 3.8.6 ([#5599](https://github.com/opensearch-project/OpenSearch/pull/5599))
- Bumps `maxmind-db` from 2.1.0 to 3.0.0 ([#5601](https://github.com/opensearch-project/OpenSearch/pull/5601))
- Bumps `protobuf-java` from 3.21.11 to 3.21.12 ([#5603](https://github.com/opensearch-project/OpenSearch/pull/5603))
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Support remote translog transfer for request level durability([#4480](https://github.com/opensearch-project/OpenSearch/pull/4480))
- Changed http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Pre conditions check before updating weighted routing metadata ([#4955](https://github.com/opensearch-project/OpenSearch/pull/4955))
- Gracefully handle concurrent zone decommission action ([#5542](https://github.com/opensearch-project/OpenSearch/pull/5542))

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
- Reject bulk requests with invalid actions ([#5299](https://github.com/opensearch-project/OpenSearch/issues/5299))
- Support OpenSSL Provider with default Netty allocator ([#5460](https://github.com/opensearch-project/OpenSearch/pull/5460))
- Increasing timeout of testQuorumRecovery to 90 seconds from 30 ([#5651](https://github.com/opensearch-project/OpenSearch/pull/5651))
- [Segment Replication] Fix for peer recovery ([#5344](https://github.com/opensearch-project/OpenSearch/pull/5344))
- [Test] Renaming PIT tests to IT to fix intermittent test failures ([#5750](https://github.com/opensearch-project/OpenSearch/pull/5750))

### Security

## [Unreleased 2.x]
### Added

### Dependencies

### Changed

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.4...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.4...2.x