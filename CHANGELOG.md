# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Add support for ignoring missing Javadoc on generated code using annotation ([#7604](https://github.com/opensearch-project/OpenSearch/pull/7604))

### Dependencies
- Bump `log4j-core` from 2.18.0 to 2.19.0
- Bump `forbiddenapis` from 3.3 to 3.4
- Bump `avro` from 1.11.0 to 1.11.1
- Bump `woodstox-core` from 6.3.0 to 6.3.1
- Bump `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bump `azure-storage-common` from 12.18.0 to 12.18.1 ([#4164](https://github.com/opensearch-project/OpenSearch/pull/4664))
- Bump `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bump `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bump `reactor-core` from 3.4.23 to 3.5.1 ([#5604](https://github.com/opensearch-project/OpenSearch/pull/5604))
- Bump `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Bump `spock-core` from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#5315](https://github.com/opensearch-project/OpenSearch/pull/5315))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))
- Update Apache Lucene to 9.5.0-snapshot-d5cef1c ([#5570](https://github.com/opensearch-project/OpenSearch/pull/5570))
- Bump `maven-model` from 3.6.2 to 3.8.6 ([#5599](https://github.com/opensearch-project/OpenSearch/pull/5599))
- Bump `maxmind-db` from 2.1.0 to 3.0.0 ([#5601](https://github.com/opensearch-project/OpenSearch/pull/5601))
- Bump `wiremock-jre8-standalone` from 2.33.2 to 2.35.0
- Bump `gson` from 2.10 to 2.10.1
- Bump `json-schema-validator` from 1.0.73 to 1.0.76
- Bump `joni` from 2.1.44 to 2.1.45
- Bump `org.jruby.joni:joni` from 2.1.45 to 2.1.48
- Bump `com.google.code.gson:gson` from 2.10 to 2.10.1
- Bump `com.maxmind.geoip2:geoip2` from 4.0.0 to 4.0.1
- Bump `com.networknt:json-schema-validator` from 1.0.76 to 1.0.78
- Bump `com.avast.gradle:gradle-docker-compose-plugin` from 0.16.11 to 0.16.12
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0
- Bump `org.apache.commons:commons-configuration2` from 2.8.0 to 2.9.0
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.3.0
- Bump `com.diffplug.spotless` from 6.17.0 to 6.18.0
- Bump `io.opencensus:opencensus-api` from 0.18.0 to 0.31.1 ([#7291](https://github.com/opensearch-project/OpenSearch/pull/7291))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Compress and cache cluster state during validate join request ([#7321](https://github.com/opensearch-project/OpenSearch/pull/7321))

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
- Support OpenSSL Provider with default Netty allocator ([#5460](https://github.com/opensearch-project/OpenSearch/pull/5460))
- Replaces ZipInputStream with ZipFile to fix Zip Slip vulnerability ([#7230](https://github.com/opensearch-project/OpenSearch/pull/7230))
- Add missing validation/parsing of SearchBackpressureMode of SearchBackpressureSettings ([#7541](https://github.com/opensearch-project/OpenSearch/pull/7541))

### Security

## [Unreleased 2.x]
### Added
- [Extensions] Moving Extensions APIs to support cross versions via protobuf. ([#7402](https://github.com/opensearch-project/OpenSearch/issues/7402))
- [Extensions] Add IdentityPlugin into core to support Extension identities ([#7246](https://github.com/opensearch-project/OpenSearch/pull/7246))
- Add connectToNodeAsExtension in TransportService ([#6866](https://github.com/opensearch-project/OpenSearch/pull/6866))
- [Search Pipelines] Accept pipelines defined in search source ([#7253](https://github.com/opensearch-project/OpenSearch/pull/7253))
- [Search Pipelines] Add `default_search_pipeline` index setting ([#7470](https://github.com/opensearch-project/OpenSearch/pull/7470))
- [Search Pipelines] Add RenameFieldResponseProcessor for Search Pipelines ([#7377](https://github.com/opensearch-project/OpenSearch/pull/7377))
- [Search Pipelines] Split search pipeline processor factories by type ([#7597](https://github.com/opensearch-project/OpenSearch/pull/7597))
- [Search Pipelines] Add script processor ([#7607](https://github.com/opensearch-project/OpenSearch/pull/7607))
- Add descending order search optimization through reverse segment read. ([#7244](https://github.com/opensearch-project/OpenSearch/pull/7244))
- Add 'unsigned_long' numeric field type ([#6237](https://github.com/opensearch-project/OpenSearch/pull/6237))
- Add back primary shard preference for queries ([#7375](https://github.com/opensearch-project/OpenSearch/pull/7375))
- Add task cancellation timestamp in task API ([#7455](https://github.com/opensearch-project/OpenSearch/pull/7455))
- Adds ExtensionsManager.lookupExtensionSettingsById ([#7466](https://github.com/opensearch-project/OpenSearch/pull/7466))
- SegRep with Remote: Add hook for publishing checkpoint notifications after segment upload to remote store ([#7394](https://github.com/opensearch-project/OpenSearch/pull/7394))
- Add search_after query optimizations with shard/segment short cutting ([#7453](https://github.com/opensearch-project/OpenSearch/pull/7453))
- Provide mechanism to configure XContent parsing constraints (after update to Jackson 2.15.0 and above) ([#7550](https://github.com/opensearch-project/OpenSearch/pull/7550))
- Support to clear filecache using clear indices cache API ([#7498](https://github.com/opensearch-project/OpenSearch/pull/7498))
- Create NamedRoute to map extension routes to a shortened name ([#6870](https://github.com/opensearch-project/OpenSearch/pull/6870))
- Added @dbwiddis as on OpenSearch maintainer ([#7665](https://github.com/opensearch-project/OpenSearch/pull/7665))
- [Extensions] Add ExtensionAwarePlugin extension point to add custom settings for extensions ([#7526](https://github.com/opensearch-project/OpenSearch/pull/7526))
- Add new cluster setting to set default index replication type ([#7420](https://github.com/opensearch-project/OpenSearch/pull/7420))

### Dependencies
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.0.0 to 12.1.3 (#7564)
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.2.0
- Bump `com.google.protobuf:protobuf-java` from 3.22.2 to 3.22.3
- Bump `jackson` from 2.14.2 to 2.15.0 ([#7286](https://github.com/opensearch-project/OpenSearch/pull/7286))
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 20.2.0 to 20.3.0
- Bump `com.netflix.nebula.ospackage-base` from 11.0.0 to 11.3.0
- Bump `gradle.plugin.com.github.johnrengelman:shadow` from 7.1.2 to 8.0.0
- Bump `jna` from 5.11.0 to 5.13.0
- Bump `commons-io:commons-io` from 2.7 to 2.12.0 (#7661, #7658, #7656)
- Bump `org.apache.shiro:shiro-core` from 1.9.1 to 1.11.0 ([#7397](https://github.com/opensearch-project/OpenSearch/pull/7397))
- Bump `jetty-server` in hdfs-fixture from 9.4.49.v20220914 to 9.4.51.v20230217 ([#7405](https://github.com/opensearch-project/OpenSearch/pull/7405))
- OpenJDK Update (April 2023 Patch releases) ([#7448](https://github.com/opensearch-project/OpenSearch/pull/7448)
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0 (#7462)
- Bump `com.azure:azure-core` from 1.34.0 to 1.39.0
- Bump `com.networknt:json-schema-validator` from 1.0.78 to 1.0.81 (#7460)
- Bump Apache Lucene to 9.6.0 ([#7505](https://github.com/opensearch-project/OpenSearch/pull/7505))
- Bump `com.google.cloud:google-cloud-core-http` from 1.93.3 to 2.17.0 (#7488)
- Bump `com.google.guava:guava` from 30.1.1-jre to 32.0.0-jre (#7565, #7811, #7808, #7807)
- Bump `com.azure:azure-storage-common` from 12.20.0 to 12.21.1 (#7566, #7814)
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0 (#7563)
- Bump `jackson` from 2.15.0 to 2.15.1 ([#7603](https://github.com/opensearch-project/OpenSearch/pull/7603))
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660)
- Bump `io.projectreactor.netty:reactor-netty-core` from 1.1.5 to 1.1.7 (#7657)
- Bump `org.apache.maven:maven-model` from 3.9.1 to 3.9.2 (#7655)
- Bump `com.google.api:gax` from 2.17.0 to 2.27.0 (#7697)
- Bump `io.projectreactor.netty:reactor-netty` from 1.1.4 to 1.1.7 ([#7725](https://github.com/opensearch-project/OpenSearch/pull/7725))
- Bump `io.projectreactor.netty:reactor-netty-http` from 1.1.4 to 1.1.7 ([#7725](https://github.com/opensearch-project/OpenSearch/pull/7725))
- Bump `com.google.http-client:google-http-client-appengine` from 1.41.8 to 1.43.2 (#7813)
- Bump `org.gradle.test-retry` from 1.5.2 to 1.5.3 (#7810)

### Changed
- Enable `./gradlew build` on MacOS by disabling bcw tests ([#7303](https://github.com/opensearch-project/OpenSearch/pull/7303))
- Moved concurrent-search from sandbox plugin to server module behind feature flag ([#7203](https://github.com/opensearch-project/OpenSearch/pull/7203))
- Allow access to indices cache clear APIs for read only indexes ([#7303](https://github.com/opensearch-project/OpenSearch/pull/7303))
- Changed concurrent-search threadpool type to be resizable and support task resource tracking ([#7502](https://github.com/opensearch-project/OpenSearch/pull/7502))
- Default search preference to _primary for searchable snapshot indices ([#7628](https://github.com/opensearch-project/OpenSearch/pull/7628))
- [Segment Replication] Remove codec name string match check for checkpoints ([#7741](https://github.com/opensearch-project/OpenSearch/pull/7741))

### Deprecated

### Removed

### Fixed
- Add more index blocks check for resize APIs ([#6774](https://github.com/opensearch-project/OpenSearch/pull/6774))
- Replaces ZipInputStream with ZipFile to fix Zip Slip vulnerability ([#7230](https://github.com/opensearch-project/OpenSearch/pull/7230))
- Add missing validation/parsing of SearchBackpressureMode of SearchBackpressureSettings ([#7541](https://github.com/opensearch-project/OpenSearch/pull/7541))
- [Search Pipelines] Better exception handling in search pipelines ([#7735](https://github.com/opensearch-project/OpenSearch/pull/7735))
- Fix input validation in segments and delete pit request ([#6645](https://github.com/opensearch-project/OpenSearch/pull/6645))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.7...2.x
