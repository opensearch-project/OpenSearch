# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Latency and Memory allocation improvements to Multi Term Aggregation queries ([#14993](https://github.com/opensearch-project/OpenSearch/pull/14993))
- Add logic in master service to optimize performance and retain detailed logging for critical cluster operations. ([#14795](https://github.com/opensearch-project/OpenSearch/pull/14795))
- Add Setting to adjust the primary constraint weights ([#16471](https://github.com/opensearch-project/OpenSearch/pull/16471))
<<<<<<< HEAD
=======
- Switch from `buildSrc/version.properties` to Gradle version catalog (`gradle/libs.versions.toml`) to enable dependabot to perform automated upgrades on common libs ([#16284](https://github.com/opensearch-project/OpenSearch/pull/16284))
- Increase segrep pressure checkpoint default limit to 30 ([#16577](https://github.com/opensearch-project/OpenSearch/pull/16577/files))
- Add dynamic setting allowing size > 0 requests to be cached in the request cache ([#16483](https://github.com/opensearch-project/OpenSearch/pull/16483))
- Support installing plugin SNAPSHOTs with SNASPHOT distribution ([#16581](https://github.com/opensearch-project/OpenSearch/pull/16581))
- Make IndexStoreListener a pluggable interface ([#16583](https://github.com/opensearch-project/OpenSearch/pull/16583))
- Support for keyword fields in star-tree index ([#16233](https://github.com/opensearch-project/OpenSearch/pull/16233))
- Add a flag in QueryShardContext to differentiate inner hit query ([#16600](https://github.com/opensearch-project/OpenSearch/pull/16600))
- Add vertical scaling and SoftReference for snapshot repository data cache ([#16489](https://github.com/opensearch-project/OpenSearch/pull/16489))
- [Workload Management] Add Workload Management IT ([#16359](https://github.com/opensearch-project/OpenSearch/pull/16359))
- Support prefix list for remote repository attributes([#16271](https://github.com/opensearch-project/OpenSearch/pull/16271))
- Add new configuration setting `synonym_analyzer`, to the `synonym` and `synonym_graph` filters, enabling the specification of a custom analyzer for reading the synonym file ([#16488](https://github.com/opensearch-project/OpenSearch/pull/16488)).
- Add stats for remote publication failure and move download failure stats to remote methods([#16682](https://github.com/opensearch-project/OpenSearch/pull/16682/))
- Update script supports java.lang.String.sha1() and java.lang.String.sha256() methods ([#16923](https://github.com/opensearch-project/OpenSearch/pull/16923))
- Added a precaution to handle extreme date values during sorting to prevent `arithmetic_exception: long overflow` ([#16812](https://github.com/opensearch-project/OpenSearch/pull/16812)).
- Add support for append only indices([#17039](https://github.com/opensearch-project/OpenSearch/pull/17039))
- Add rule schema changes for rule based auto tagging ([#17238](https://github.com/opensearch-project/OpenSearch/pull/17238))
- Add `verbose_pipeline` parameter to output each processor's execution details ([#16843](https://github.com/opensearch-project/OpenSearch/pull/16843)).
- Add search replica stats to segment replication stats API ([#16678](https://github.com/opensearch-project/OpenSearch/pull/16678))
>>>>>>> bb8d06c5d59 (modify rule structure based on comment)
- Introduce a setting to disable download of full cluster state from remote on term mismatch([#16798](https://github.com/opensearch-project/OpenSearch/pull/16798/))
- Added ability to retrieve value from DocValues in a flat_object filed([#16802](https://github.com/opensearch-project/OpenSearch/pull/16802))
- Improve performace of NumericTermAggregation by avoiding unnecessary sorting([#17252](https://github.com/opensearch-project/OpenSearch/pull/17252))
- [Rule Based Auto-tagging] Add rule schema for auto tagging ([#17238](https://github.com/opensearch-project/OpenSearch/pull/17238))
- Fix Bug - Handle unsigned long in sorting order assertion of LongHashSet ([#17207](https://github.com/opensearch-project/OpenSearch/pull/17207))
- Implemented computation of segment replication stats at shard level ([#17055](https://github.com/opensearch-project/OpenSearch/pull/17055))
- [Rule Based Auto-tagging] Add in-memory attribute value store ([#17342](https://github.com/opensearch-project/OpenSearch/pull/17342))

### Dependencies
- Bump `org.awaitility:awaitility` from 4.2.0 to 4.3.0 ([#17230](https://github.com/opensearch-project/OpenSearch/pull/17230), [#17439](https://github.com/opensearch-project/OpenSearch/pull/17439))
- Bump `dnsjava:dnsjava` from 3.6.2 to 3.6.3 ([#17231](https://github.com/opensearch-project/OpenSearch/pull/17231))
- Bump `com.google.code.gson:gson` from 2.11.0 to 2.12.1 ([#17229](https://github.com/opensearch-project/OpenSearch/pull/17229))
- Bump `org.jruby.joni:joni` from 2.2.1 to 2.2.3 ([#17136](https://github.com/opensearch-project/OpenSearch/pull/17136))
- Bump `org.apache.ant:ant` from 1.10.14 to 1.10.15 ([#17288](https://github.com/opensearch-project/OpenSearch/pull/17288))
- Bump `reactor_netty` from 1.1.26 to 1.1.27 ([#17322](https://github.com/opensearch-project/OpenSearch/pull/17322))
- Bump `me.champeau.gradle.japicmp` from 0.4.5 to 0.4.6 ([#17375](https://github.com/opensearch-project/OpenSearch/pull/17375))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.37.1 to 2.52.0 ([#17379](https://github.com/opensearch-project/OpenSearch/pull/17379))
- Bump `net.minidev:json-smart` from 2.5.1 to 2.5.2 ([#17378](https://github.com/opensearch-project/OpenSearch/pull/17378))
- Bump `com.netflix.nebula.ospackage-base` from 11.10.1 to 11.11.1 ([#17374](https://github.com/opensearch-project/OpenSearch/pull/17374))
- Bump `software.amazon.awssdk` from 2.20.86 to 2.30.31 ([17396](https://github.com/opensearch-project/OpenSearch/pull/17396))
- Bump `org.jruby.jcodings:jcodings` from 1.0.61 to 1.0.63 ([#17560](https://github.com/opensearch-project/OpenSearch/pull/17560))
- Bump `com.azure:azure-storage-blob` from 12.28.1 to 12.29.1 ([#17562](https://github.com/opensearch-project/OpenSearch/pull/17562))

### Changed
- Convert transport-reactor-netty4 to use gradle version catalog [#17233](https://github.com/opensearch-project/OpenSearch/pull/17233)
- Increase force merge threads to 1/8th of cores [#17255](https://github.com/opensearch-project/OpenSearch/pull/17255)
- TieredSpilloverCache took-time threshold now guards heap tier as well as disk tier [#17190](https://github.com/opensearch-project/OpenSearch/pull/17190)

### Deprecated

### Removed

### Fixed
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix exists queries on nested flat_object fields throws exception ([#16803](https://github.com/opensearch-project/OpenSearch/pull/16803))
- Add highlighting for wildcard search on `match_only_text` field ([#17101](https://github.com/opensearch-project/OpenSearch/pull/17101))
- Fix illegal argument exception when creating a PIT ([#16781](https://github.com/opensearch-project/OpenSearch/pull/16781))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.19...2.x
