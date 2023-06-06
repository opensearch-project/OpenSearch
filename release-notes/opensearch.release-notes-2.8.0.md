## 2023-06-06 Version 2.8.0 Release Notes

## [2.8]
### Added
- [Extensions] Moving Extensions APIs to support cross versions via protobuf. ([#7402](https://github.com/opensearch-project/OpenSearch/issues/7402))
- [Extensions] Add IdentityPlugin into core to support Extension identities ([#7246](https://github.com/opensearch-project/OpenSearch/pull/7246))
- Add connectToNodeAsExtension in TransportService ([#6866](https://github.com/opensearch-project/OpenSearch/pull/6866))
- [Search Pipelines] Accept pipelines defined in search source ([#7253](https://github.com/opensearch-project/OpenSearch/pull/7253))
- [Search Pipelines] Add `default_search_pipeline` index setting ([#7470](https://github.com/opensearch-project/OpenSearch/pull/7470))
- [Search Pipelines] Add RenameFieldResponseProcessor for Search Pipelines ([#7377](https://github.com/opensearch-project/OpenSearch/pull/7377))
- [Search Pipelines] Split search pipeline processor factories by type ([#7597](https://github.com/opensearch-project/OpenSearch/pull/7597))
- [Search Pipelines] Add script processor ([#7607](https://github.com/opensearch-project/OpenSearch/pull/7607))
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
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660, #7812)
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

### Fixed
- Add more index blocks check for resize APIs ([#6774](https://github.com/opensearch-project/OpenSearch/pull/6774))
- Replaces ZipInputStream with ZipFile to fix Zip Slip vulnerability ([#7230](https://github.com/opensearch-project/OpenSearch/pull/7230))
- Add missing validation/parsing of SearchBackpressureMode of SearchBackpressureSettings ([#7541](https://github.com/opensearch-project/OpenSearch/pull/7541))
- [Search Pipelines] Better exception handling in search pipelines ([#7735](https://github.com/opensearch-project/OpenSearch/pull/7735))
- Fix input validation in segments and delete pit request ([#6645](https://github.com/opensearch-project/OpenSearch/pull/6645))

