# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Change priority for scheduling reroute during timeout([#16445](https://github.com/opensearch-project/OpenSearch/pull/16445))
- [Rule Based Auto-tagging] Add rule schema for auto tagging ([#17238](https://github.com/opensearch-project/OpenSearch/pull/17238))
- Renaming the node role search to warm ([#17573](https://github.com/opensearch-project/OpenSearch/pull/17573))
- Introduce a new search node role to hold search only shards ([#17620](https://github.com/opensearch-project/OpenSearch/pull/17620))
- Fix systemd integTest on deb regarding path ownership check ([#17641](https://github.com/opensearch-project/OpenSearch/pull/17641)) 
- Add dfs transformation function in XContentMapValues ([#17612](https://github.com/opensearch-project/OpenSearch/pull/17612))

### Changed
- Migrate BC libs to their FIPS counterparts ([#14912](https://github.com/opensearch-project/OpenSearch/pull/14912))

### Dependencies
- Bump `com.nimbusds:nimbus-jose-jwt` from 9.41.1 to 10.0.2 ([#17607](https://github.com/opensearch-project/OpenSearch/pull/17607))
- Bump `com.google.api:api-common` from 1.8.1 to 2.46.1 ([#17604](https://github.com/opensearch-project/OpenSearch/pull/17604))
- Bump `ch.qos.logback:logback-core` from 1.5.16 to 1.5.17 ([#17609](https://github.com/opensearch-project/OpenSearch/pull/17609))
- Bump `org.jruby.joni:joni` from 2.2.3 to 2.2.5 ([#17608](https://github.com/opensearch-project/OpenSearch/pull/17608))
- Bump `dangoslen/dependabot-changelog-helper` from 3 to 4 ([#17498](https://github.com/opensearch-project/OpenSearch/pull/17498))
- Bump `com.google.api:gax` from 2.35.0 to 2.63.1 ([#17465](https://github.com/opensearch-project/OpenSearch/pull/17465))

### Changed

### Deprecated

### Removed

### Fixed
- Fix bytes parameter on `_cat/recovery` ([#17598](https://github.com/opensearch-project/OpenSearch/pull/17598))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/f58d846f...main
