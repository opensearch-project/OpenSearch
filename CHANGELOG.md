# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add server version as REST response header [#6583](https://github.com/opensearch-project/OpenSearch/issues/6583)
- Start replication checkpointTimers on primary before segments upload to remote store. ([#8221]()https://github.com/opensearch-project/OpenSearch/pull/8221)
- Introduce new static cluster setting to control slice computation for concurrent segment search. ([#8847](https://github.com/opensearch-project/OpenSearch/pull/8884))
- Add configuration for file cache size to max remote data ratio to prevent oversubscription of file cache ([#8606](https://github.com/opensearch-project/OpenSearch/pull/8606))

### Dependencies
- Bump `com.azure:azure-storage-common` from 12.21.0 to 12.21.1 (#7566, #7814)
- Bump `com.google.guava:guava` from 30.1.1-jre to 32.1.1-jre (#7565, #7811, #7807, #7808, #8402, #8400, #8401, #8581)
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660, #7812)
- Bump `org.gradle.test-retry` from 1.5.2 to 1.5.3 (#7810)
- Bump `com.diffplug.spotless` from 6.17.0 to 6.18.0 (#7896)
- Bump `jackson` from 2.15.1 to 2.15.2 ([#7897](https://github.com/opensearch-project/OpenSearch/pull/7897))
- Add `com.github.luben:zstd-jni` version 1.5.5-3 ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Bump `netty` from 4.1.91.Final to 4.1.93.Final ([#7901](https://github.com/opensearch-project/OpenSearch/pull/7901))
- Bump `com.amazonaws` 1.12.270 to `software.amazon.awssdk` 2.20.55 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Add `org.reactivestreams` 1.0.4 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Bump `com.networknt:json-schema-validator` from 1.0.81 to 1.0.85 ([7968], #8255)
- Bump `com.netflix.nebula:gradle-extra-configurations-plugin` from 9.0.0 to 10.0.0 in /buildSrc ([#7068](https://github.com/opensearch-project/OpenSearch/pull/7068))
- Bump `com.google.guava:guava` from 32.0.0-jre to 32.0.1-jre (#8009)
- Bump `commons-io:commons-io` from 2.12.0 to 2.13.0 (#8014, #8013, #8010)
- Bump `com.diffplug.spotless` from 6.18.0 to 6.19.0 (#8007)
- Bump `'com.azure:azure-storage-blob` to 12.22.2 from 12.21.1 ([#8043](https://github.com/opensearch-project/OpenSearch/pull/8043))
- Bump `org.jruby.joni:joni` from 2.1.48 to 2.2.1 (#8015, #8254)
- Bump `com.google.guava:guava` from 32.0.0-jre to 32.0.1-jre ([#8011](https://github.com/opensearch-project/OpenSearch/pull/8011), [#8012](https://github.com/opensearch-project/OpenSearch/pull/8012), [#8107](https://github.com/opensearch-project/OpenSearch/pull/8107))
- Bump `io.projectreactor:reactor-core` from 3.4.18 to 3.5.6 in /plugins/repository-azure ([#8016](https://github.com/opensearch-project/OpenSearch/pull/8016))
- Bump `spock-core` from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#8122](https://github.com/opensearch-project/OpenSearch/pull/8122))
- Bump `com.networknt:json-schema-validator` from 1.0.83 to 1.0.84 (#8141)
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.3 to 12.1.5 (#8139, #8568)
- Bump `commons-io:commons-io` from 2.12.0 to 2.13.0 in /plugins/discovery-azure-classic ([#8140](https://github.com/opensearch-project/OpenSearch/pull/8140))
- Bump `mockito` from 5.2.0 to 5.4.0 ([#8181](https://github.com/opensearch-project/OpenSearch/pull/8181))
- Bump `netty` from 4.1.93.Final to 4.1.94.Final ([#8191](https://github.com/opensearch-project/OpenSearch/pull/8191))
- Bump `org.apache.hadoop:hadoop-minicluster` from 3.3.5 to 3.3.6 (#8257)
- Bump `io.projectreactor.netty:reactor-netty-http` from 1.1.7 to 1.1.8 (#8256)
- [Upgrade] Lucene 9.7.0 release (#8272)
- Bump `org.jboss.resteasy:resteasy-jackson2-provider` from 3.0.26.Final to 6.2.4.Final in /qa/wildfly ([#8209](https://github.com/opensearch-project/OpenSearch/pull/8209))
- Bump `com.google.api-client:google-api-client` from 1.34.0 to 2.2.0 ([#8276](https://github.com/opensearch-project/OpenSearch/pull/8276))
- Update Apache HttpCore/ HttpClient and Apache HttpCore5 / HttpClient5 dependencies ([#8434](https://github.com/opensearch-project/OpenSearch/pull/8434))
- Bump `org.apache.maven:maven-model` from 3.9.2 to 3.9.3 (#8403)
- Bump `io.projectreactor.netty:reactor-netty` and `io.projectreactor.netty:reactor-netty-core` from 1.1.7 to 1.1.8 (#8405)
- Bump `com.azure:azure-storage-blob` from 12.22.2 to 12.22.3 (#8572)
- Bump `net.minidev:json-smart` from 2.4.11 to 2.5.0 (#8575, #8576)
- Bump `com.google.jimfs:jimfs` from 1.2 to 1.3.0 (#8577, #8571)
- Bump `com.networknt:json-schema-validator` from 1.0.85 to 1.0.86 ([#8573](https://github.com/opensearch-project/OpenSearch/pull/8573))
- Bump `com.google.cloud:google-cloud-core-http` from 2.17.0 to 2.21.0 ([#8586](https://github.com/opensearch-project/OpenSearch/pull/8586))
- Bump `com.google.jimfs:jimfs` from 1.2 to 1.3.0 ([#8585](https://github.com/opensearch-project/OpenSearch/pull/8585))
- Bump `org.apache.logging.log4j:log4j-core` from 2.17.1 to 2.20.0 ([#8307](https://github.com/opensearch-project/OpenSearch/pull/8307))
- Bump `io.grpc:grpc-context` from 1.46.0 to 1.56.1 ([#8726](https://github.com/opensearch-project/OpenSearch/pull/8726))
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.5 to 12.1.6 ([#8724](https://github.com/opensearch-project/OpenSearch/pull/8724))
- Bump `commons-codec:commons-codec` from 1.15 to 1.16.0 ([#8725](https://github.com/opensearch-project/OpenSearch/pull/8725))
- Bump `org.apache.zookeeper:zookeeper` from 3.8.1 to 3.8.2 ([#8844](https://github.com/opensearch-project/OpenSearch/pull/8844))
- Bump `org.gradle.test-retry` from 1.5.3 to 1.5.4 ([#8842](https://github.com/opensearch-project/OpenSearch/pull/8842))
- Bump `com.netflix.nebula.ospackage-base` from 11.3.0 to 11.4.0 ([#8838](https://github.com/opensearch-project/OpenSearch/pull/8838))
- Bump `com.google.http-client:google-http-client-gson` from 1.43.2 to 1.43.3 ([#8840](https://github.com/opensearch-project/OpenSearch/pull/8840))
- OpenJDK Update (July 2023 Patch releases) ([#8869](https://github.com/opensearch-project/OpenSearch/pull/8869))
- Bump `hadoop` libraries from 3.3.4 to 3.3.6 ([#6995](https://github.com/opensearch-project/OpenSearch/pull/6995))
- Bump `com.gradle.enterprise` from 3.13.3 to 3.14.1 ([#8996](https://github.com/opensearch-project/OpenSearch/pull/8996))
- Bump `org.apache.commons:commons-lang3` from 3.12.0 to 3.13.0 ([#8995](https://github.com/opensearch-project/OpenSearch/pull/8995))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.0 to 2.21.1 ([#8999](https://github.com/opensearch-project/OpenSearch/pull/8999))
- Bump `com.maxmind.geoip2:geoip2` from 4.0.1 to 4.1.0 ([#8998](https://github.com/opensearch-project/OpenSearch/pull/8998))

### Changed
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))
- Make Span exporter configurable ([#8620](https://github.com/opensearch-project/OpenSearch/issues/8620))
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))
- [Refactor] StreamIO from common to core.common namespace in core lib ([#8157](https://github.com/opensearch-project/OpenSearch/pull/8157))
- [Refactor] Remaining HPPC to java.util collections ([#8730](https://github.com/opensearch-project/OpenSearch/pull/8730))
- Remote Segment Store Repository setting moved from `index.remote_store.repository` to `index.remote_store.segment.repository` and `cluster.remote_store.repository` to `cluster.remote_store.segment.repository` respectively for Index and Cluster level settings ([#8719](https://github.com/opensearch-project/OpenSearch/pull/8719))
- Change InternalSignificantTerms to sum shard-level superset counts only in final reduce ([#8735](https://github.com/opensearch-project/OpenSearch/pull/8735))
- Exclude 'benchmarks' from codecov report ([#8805](https://github.com/opensearch-project/OpenSearch/pull/8805))
- Create separate SourceLookup instance per segment slice in SignificantTextAggregatorFactory ([#8807](https://github.com/opensearch-project/OpenSearch/pull/8807))
- Replace the deprecated IndexReader APIs with new storedFields() & termVectors() ([#7792](https://github.com/opensearch-project/OpenSearch/pull/7792))
- [Remote Store] Add support to restore only unassigned shards of an index ([#8792](https://github.com/opensearch-project/OpenSearch/pull/8792))
- Add safeguard limits for file cache during node level allocation ([#8208](https://github.com/opensearch-project/OpenSearch/pull/8208))
- Add support for aggregation profiler with concurrent aggregation ([#8801](https://github.com/opensearch-project/OpenSearch/pull/8801))
- [Remove] Deprecated Fractional ByteSizeValue support #9005 ([#9005](https://github.com/opensearch-project/OpenSearch/pull/9005))
- Add support for aggregation profiler with concurrent aggregation ([#8801](https://github.com/opensearch-project/OpenSearch/pull/8801))

### Deprecated

### Removed
- Remove provision to create Remote Indices without Remote Translog Store ([#8719](https://github.com/opensearch-project/OpenSearch/pull/8719))

### Fixed

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.9...2.x
