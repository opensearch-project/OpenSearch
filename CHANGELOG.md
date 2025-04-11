# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Improve performace of NumericTermAggregation by avoiding unnecessary sorting([#17252](https://github.com/opensearch-project/OpenSearch/pull/17252))
- [Rule Based Auto-tagging] Add rule schema for auto tagging ([#17238](https://github.com/opensearch-project/OpenSearch/pull/17238))
- Add execution_hint to cardinality aggregator request (#[17419](https://github.com/opensearch-project/OpenSearch/pull/17419))
- Fix Bug - Handle unsigned long in sorting order assertion of LongHashSet ([#17207](https://github.com/opensearch-project/OpenSearch/pull/17207))
- [Rule Based Auto-tagging] Add in-memory attribute value store ([#17342](https://github.com/opensearch-project/OpenSearch/pull/17342))
- [Rule Based Auto-tagging] Add in-memory rule processing service ([#17365](https://github.com/opensearch-project/OpenSearch/pull/17365))
- Change priority for scheduling reroute during timeout([#16445](https://github.com/opensearch-project/OpenSearch/pull/16445))

### Dependencies
- Bump `dnsjava:dnsjava` from 3.6.2 to 3.6.3 ([#17231](https://github.com/opensearch-project/OpenSearch/pull/17231))
- Bump `com.google.code.gson:gson` from 2.11.0 to 2.12.1 ([#17229](https://github.com/opensearch-project/OpenSearch/pull/17229))
- Bump `org.jruby.joni:joni` from 2.2.1 to 2.2.3 ([#17136](https://github.com/opensearch-project/OpenSearch/pull/17136))
- Bump `org.apache.ant:ant` from 1.10.14 to 1.10.15 ([#17288](https://github.com/opensearch-project/OpenSearch/pull/17288))
- Bump netty from 4.1.117.Final to 4.1.118.Final ([#17320](https://github.com/opensearch-project/OpenSearch/pull/17320))
- Bump `reactor_netty` from 1.1.26 to 1.1.27 ([#17322](https://github.com/opensearch-project/OpenSearch/pull/17322))
- Bump `me.champeau.gradle.japicmp` from 0.4.5 to 0.4.6 ([#17375](https://github.com/opensearch-project/OpenSearch/pull/17375))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.37.1 to 2.52.0 ([#17379](https://github.com/opensearch-project/OpenSearch/pull/17379))
- Bump `net.minidev:json-smart` from 2.5.1 to 2.5.2 ([#17378](https://github.com/opensearch-project/OpenSearch/pull/17378))
- Bump `com.netflix.nebula.ospackage-base` from 11.10.1 to 11.11.1 ([#17374](https://github.com/opensearch-project/OpenSearch/pull/17374))
- Bump `ch.qos.logback:logback-classic` from 1.5.16 to 1.5.17 ([#17497](https://github.com/opensearch-project/OpenSearch/pull/17497))
- Bump `software.amazon.awssdk` from 2.20.86 to 2.30.31 ([17396](https://github.com/opensearch-project/OpenSearch/pull/17396))
- Bump `org.jruby.jcodings:jcodings` from 1.0.61 to 1.0.63 ([#17560](https://github.com/opensearch-project/OpenSearch/pull/17560))
- Bump `com.azure:azure-storage-blob` from 12.28.1 to 12.30.0 ([#17562](https://github.com/opensearch-project/OpenSearch/pull/17562), [#17667](https://github.com/opensearch-project/OpenSearch/pull/17667))

### Changed
- Convert transport-reactor-netty4 to use gradle version catalog [#17233](https://github.com/opensearch-project/OpenSearch/pull/17233)
- Increase force merge threads to 1/8th of cores [#17255](https://github.com/opensearch-project/OpenSearch/pull/17255)
- TieredSpilloverCache took-time threshold now guards heap tier as well as disk tier [#17190](https://github.com/opensearch-project/OpenSearch/pull/17190)

### Deprecated

### Removed
- Remove FeatureFlags.PLUGGABLE_CACHE as the feature is no longer experimental ([#17344](https://github.com/opensearch-project/OpenSearch/pull/17344))

### Fixed
- Fix visit of inner query for FunctionScoreQueryBuilder ([#16776](https://github.com/opensearch-project/OpenSearch/pull/16776))
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix illegal argument exception when creating a PIT ([#16781](https://github.com/opensearch-project/OpenSearch/pull/16781))
- Fix NPE in node stats due to QueryGroupTasks ([#17576](https://github.com/opensearch-project/OpenSearch/pull/17576))
- Fix exists queries on nested flat_object fields throws exception ([#16803](https://github.com/opensearch-project/OpenSearch/pull/16803))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.19...2.x
