# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Latency and Memory allocation improvements to Multi Term Aggregation queries ([#14993](https://github.com/opensearch-project/OpenSearch/pull/14993))
- Add logic in master service to optimize performance and retain detailed logging for critical cluster operations. ([#14795](https://github.com/opensearch-project/OpenSearch/pull/14795))
- Add Setting to adjust the primary constraint weights ([#16471](https://github.com/opensearch-project/OpenSearch/pull/16471))
- Introduce a setting to disable download of full cluster state from remote on term mismatch([#16798](https://github.com/opensearch-project/OpenSearch/pull/16798/))
- Added ability to retrieve value from DocValues in a flat_object filed([#16802](https://github.com/opensearch-project/OpenSearch/pull/16802))
- Improve performace of NumericTermAggregation by avoiding unnecessary sorting([#17252](https://github.com/opensearch-project/OpenSearch/pull/17252))
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
- Bump `dangoslen/dependabot-changelog-helper` from 3 to 4 ([#17498](https://github.com/opensearch-project/OpenSearch/pull/17498))

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
