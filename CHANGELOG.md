# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Extensions] Moving Extensions APIs to support cross versions via protobuf. ([#7402](https://github.com/opensearch-project/OpenSearch/issues/7402))
- [Extensions] Add IdentityPlugin into core to support Extension identities ([#7246](https://github.com/opensearch-project/OpenSearch/pull/7246))
- Add connectToNodeAsExtension in TransportService ([#6866](https://github.com/opensearch-project/OpenSearch/pull/6866))
- [Search Pipelines] Accept pipelines defined in search source ([#7253](https://github.com/opensearch-project/OpenSearch/pull/7253))
- Add descending order search optimization through reverse segment read. ([#7244](https://github.com/opensearch-project/OpenSearch/pull/7244))
- Add 'unsigned_long' numeric field type ([#6237](https://github.com/opensearch-project/OpenSearch/pull/6237))
- Add back primary shard preference for queries ([#7375](https://github.com/opensearch-project/OpenSearch/pull/7375))
- Add descending order search optimization through reverse segment read. ([#7244](https://github.com/opensearch-project/OpenSearch/pull/7244))
- Adds ExtensionsManager.lookupExtensionSettingsById ([#7466](https://github.com/opensearch-project/OpenSearch/pull/7466))
- Provide mechanism to configure XContent parsing constraints (after update to Jackson 2.15.0 and above) ([#7550](https://github.com/opensearch-project/OpenSearch/pull/7550))
- Support to clear filecache using clear indices cache API ([#7498](https://github.com/opensearch-project/OpenSearch/pull/7498))

### Dependencies
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.0.0 to 12.1.3 (#7564)
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.2.0
- Bump `com.google.protobuf:protobuf-java` from 3.22.2 to 3.22.3
- Bump `jackson` from 2.14.2 to 2.15.0 ([#7286](https://github.com/opensearch-project/OpenSearch/pull/7286))
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 20.2.0 to 20.3.0
- Bump `com.netflix.nebula.ospackage-base` from 11.0.0 to 11.3.0
- Bump `gradle.plugin.com.github.johnrengelman:shadow` from 7.1.2 to 8.0.0
- Bump `jna` from 5.11.0 to 5.13.0
- Bump `commons-io:commons-io` from 2.7 to 2.11.0
- Bump `org.apache.shiro:shiro-core` from 1.9.1 to 1.11.0 ([#7397](https://github.com/opensearch-project/OpenSearch/pull/7397))
- Bump `jetty-server` in hdfs-fixture from 9.4.49.v20220914 to 9.4.51.v20230217 ([#7405](https://github.com/opensearch-project/OpenSearch/pull/7405))
- OpenJDK Update (April 2023 Patch releases) ([#7448](https://github.com/opensearch-project/OpenSearch/pull/7448)
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0 (#7462)
- Bump `com.azure:azure-core` from 1.34.0 to 1.39.0
- Bump `com.networknt:json-schema-validator` from 1.0.78 to 1.0.81 (#7460)
- Bump Apache Lucene to 9.6.0 ([#7505](https://github.com/opensearch-project/OpenSearch/pull/7505))
- Bump `com.google.cloud:google-cloud-core-http` from 1.93.3 to 2.17.0 (#7488)
- Bump `com.google.guava:guava` from 30.1.1-jre to 31.1-jre (#7565)
- Bump `com.azure:azure-storage-common` from 12.20.0 to 12.21.0 (#7566)
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0 (#7563)

### Changed
- Enable `./gradlew build` on MacOS by disabling bcw tests ([#7303](https://github.com/opensearch-project/OpenSearch/pull/7303))
- Moved concurrent-search from sandbox plugin to server module behind feature flag ([#7203](https://github.com/opensearch-project/OpenSearch/pull/7203))
- Allow access to indices cache clear APIs for read only indexes ([#7303](https://github.com/opensearch-project/OpenSearch/pull/7303))

### Deprecated

### Removed

### Fixed
- Replaces ZipInputStream with ZipFile to fix Zip Slip vulnerability ([#7230](https://github.com/opensearch-project/OpenSearch/pull/7230))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.7...2.x
