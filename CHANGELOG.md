# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Improve performace of NumericTermAggregation by avoiding unnecessary sorting([#17252](https://github.com/opensearch-project/OpenSearch/pull/17252))
- Add execution_hint to cardinality aggregator request (#[TBD](TBD))

### Dependencies
- Bump `dnsjava:dnsjava` from 3.6.2 to 3.6.3 ([#17231](https://github.com/opensearch-project/OpenSearch/pull/17231))
- Bump `com.google.code.gson:gson` from 2.11.0 to 2.12.1 ([#17229](https://github.com/opensearch-project/OpenSearch/pull/17229))
- Bump `org.jruby.joni:joni` from 2.2.1 to 2.2.3 ([#17136](https://github.com/opensearch-project/OpenSearch/pull/17136))
- Bump `org.apache.ant:ant` from 1.10.14 to 1.10.15 ([#17288](https://github.com/opensearch-project/OpenSearch/pull/17288))
- Bump netty from 4.1.117.Final to 4.1.118.Final ([#17320](https://github.com/opensearch-project/OpenSearch/pull/17320))
- Bump `reactor_netty` from 1.1.26 to 1.1.27 ([#17322](https://github.com/opensearch-project/OpenSearch/pull/17322))
- Bump `me.champeau.gradle.japicmp` from 0.4.5 to 0.4.6 ([#17375](https://github.com/opensearch-project/OpenSearch/pull/17375))
- Bump `jetty` version from 9.4.55.v20240627 to 9.4.57.v20241219

### Changed
- Convert transport-reactor-netty4 to use gradle version catalog [#17233](https://github.com/opensearch-project/OpenSearch/pull/17233)
- Increase force merge threads to 1/8th of cores [#17255](https://github.com/opensearch-project/OpenSearch/pull/17255)

### Deprecated

### Removed

### Fixed
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix illegal argument exception when creating a PIT ([#16781](https://github.com/opensearch-project/OpenSearch/pull/16781))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.19...2.x
