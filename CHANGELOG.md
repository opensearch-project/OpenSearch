# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add task cancellation monitoring service ([#7642](https://github.com/opensearch-project/OpenSearch/pull/7642))
- Add TokenManager Interface ([#7452](https://github.com/opensearch-project/OpenSearch/pull/7452))
- Add Remote store as a segment replication source ([#7653](https://github.com/opensearch-project/OpenSearch/pull/7653))
- Implement concurrent aggregations support without profile option ([#7514](https://github.com/opensearch-project/OpenSearch/pull/7514))
- Add dynamic index and cluster setting for concurrent segment search ([#7956](https://github.com/opensearch-project/OpenSearch/pull/7956))
- Add descending order search optimization through reverse segment read. ([#7967](https://github.com/opensearch-project/OpenSearch/pull/7967))
- Update components of segrep backpressure to support remote store. ([#8020](https://github.com/opensearch-project/OpenSearch/pull/8020))
- Make remote cluster connection setup in async ([#8038](https://github.com/opensearch-project/OpenSearch/pull/8038))
- Add API to initialize extensions ([#8029]()https://github.com/opensearch-project/OpenSearch/pull/8029)

### Dependencies
- Bump `com.azure:azure-storage-common` from 12.21.0 to 12.21.1 (#7566, #7814)
- Bump `com.google.guava:guava` from 30.1.1-jre to 32.0.0-jre (#7565, #7811, #7807, #7808)
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
- Bump `org.jruby.joni:joni` from 2.1.48 to 2.2.1 (#8015)
- Bump `com.google.guava:guava` from 32.0.0-jre to 32.0.1-jre ([#8011](https://github.com/opensearch-project/OpenSearch/pull/8011), [#8012](https://github.com/opensearch-project/OpenSearch/pull/8012), [#8107](https://github.com/opensearch-project/OpenSearch/pull/8107))
- Bump `io.projectreactor:reactor-core` from 3.4.18 to 3.5.6 in /plugins/repository-azure ([#8016](https://github.com/opensearch-project/OpenSearch/pull/8016))
- Bump `spock-core` from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#8122](https://github.com/opensearch-project/OpenSearch/pull/8122))
- Bump `com.networknt:json-schema-validator` from 1.0.83 to 1.0.84 (#8141)
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.3 to 12.1.4 (#8139)
- Bump `commons-io:commons-io` from 2.12.0 to 2.13.0 in /plugins/discovery-azure-classic ([#8140](https://github.com/opensearch-project/OpenSearch/pull/8140))
- Bump `mockito` from 5.2.0 to 5.4.0 ([#8181](https://github.com/opensearch-project/OpenSearch/pull/8181))
- Bump `netty` from 4.1.93.Final to 4.1.94.Final ([#8191](https://github.com/opensearch-project/OpenSearch/pull/8191))

### Changed
- Replace jboss-annotations-api_1.2_spec with jakarta.annotation-api ([#7836](https://github.com/opensearch-project/OpenSearch/pull/7836))
- Reduce memory copy in zstd compression ([#7681](https://github.com/opensearch-project/OpenSearch/pull/7681))
- Add min, max, average and thread info to resource stats in tasks API ([#7673](https://github.com/opensearch-project/OpenSearch/pull/7673))
- Add ZSTD compression for snapshotting ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Change `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` to `aws.ec2MetadataServiceEndpoint` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Change `com.amazonaws.sdk.stsEndpointOverride` to `aws.stsEndpointOverride` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Add new query profile collector fields with concurrent search execution ([#7898](https://github.com/opensearch-project/OpenSearch/pull/7898))
- Align range and default value for deletes_pct_allowed in merge policy ([#7730](https://github.com/opensearch-project/OpenSearch/pull/7730))
- Rename QueryPhase actors like Suggest, Rescore to be processors rather than phase ([#8025](https://github.com/opensearch-project/OpenSearch/pull/8025))
- [Snapshot Interop] Add Changes in Create Snapshot Flow for remote store interoperability. ([#8071](https://github.com/opensearch-project/OpenSearch/pull/8071))
- Allow insecure string settings to warn-log usage and advise to migration of a newer secure variant ([#5496](https://github.com/opensearch-project/OpenSearch/pull/5496))
- Pass localNode info to all plugins on node start ([#7919](https://github.com/opensearch-project/OpenSearch/pull/7919)
- Compress and cache cluster state during validate join request ([#7321](https://github.com/opensearch-project/OpenSearch/pull/7321))
- [Refactor] Sets util from server to common lib ([#8230](https://github.com/opensearch-project/OpenSearch/pull/8230))
- [Refactor] Metadata members from ImmutableOpenMap to j.u.Map ([#7165](https://github.com/opensearch-project/OpenSearch/pull/7165))
- [Refactor] more ImmutableOpenMap to jdk Map in cluster package ([#7301](https://github.com/opensearch-project/OpenSearch/pull/7301))
- [Refactor] ImmutableOpenMap to j.u.Map in IndexMetadata ([#7306](https://github.com/opensearch-project/OpenSearch/pull/7306))

### Deprecated

### Removed
- Remove `COMPRESSOR` variable from `CompressorFactory` and use `DEFLATE_COMPRESSOR` instead ([7907](https://github.com/opensearch-project/OpenSearch/pull/7907))

### Fixed
- Fixing error: adding a new/forgotten parameter to the configuration for checking the config on startup in plugins/repository-s3 #7924
- Enforce 512 byte document ID limit in bulk updates ([#8039](https://github.com/opensearch-project/OpenSearch/pull/8039))
- With only GlobalAggregation in request causes unnecessary wrapping with MultiCollector ([#8125](https://github.com/opensearch-project/OpenSearch/pull/8125))
- Fix mapping char_filter when mapping a hashtag ([#7591](https://github.com/opensearch-project/OpenSearch/pull/7591))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.8...2.x
