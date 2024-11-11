# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Latency and Memory allocation improvements to Multi Term Aggregation queries ([#14993](https://github.com/opensearch-project/OpenSearch/pull/14993))
- Add support for restoring from snapshot with search replicas ([#16111](https://github.com/opensearch-project/OpenSearch/pull/16111))
- Ensure support of the transport-nio by security plugin ([#16474](https://github.com/opensearch-project/OpenSearch/pull/16474))
- Add logic in master service to optimize performance and retain detailed logging for critical cluster operations. ([#14795](https://github.com/opensearch-project/OpenSearch/pull/14795))
- Add Setting to adjust the primary constraint weights ([#16471](https://github.com/opensearch-project/OpenSearch/pull/16471))
- Switch from `buildSrc/version.properties` to Gradle version catalog (`gradle/libs.versions.toml`) to enable dependabot to perform automated upgrades on common libs ([#16284](https://github.com/opensearch-project/OpenSearch/pull/16284))
- Increase segrep pressure checkpoint default limit to 30 ([#16577](https://github.com/opensearch-project/OpenSearch/pull/16577/files))
- Add dynamic setting allowing size > 0 requests to be cached in the request cache ([#16483](https://github.com/opensearch-project/OpenSearch/pull/16483))
- Make IndexStoreListener a pluggable interface ([#16583](https://github.com/opensearch-project/OpenSearch/pull/16583))

### Dependencies
- Bump `com.azure:azure-storage-common` from 12.25.1 to 12.27.1 ([#16521](https://github.com/opensearch-project/OpenSearch/pull/16521))
- Bump `com.google.apis:google-api-services-compute` from v1-rev20240407-2.0.0 to v1-rev20241021-2.0.0 ([#16502](https://github.com/opensearch-project/OpenSearch/pull/16502), [#16548](https://github.com/opensearch-project/OpenSearch/pull/16548))
- Bump `com.azure:azure-storage-blob` from 12.23.0 to 12.28.1 ([#16501](https://github.com/opensearch-project/OpenSearch/pull/16501))
- Bump `org.apache.hadoop:hadoop-minicluster` from 3.4.0 to 3.4.1 ([#16550](https://github.com/opensearch-project/OpenSearch/pull/16550))
- Bump `org.apache.xmlbeans:xmlbeans` from 5.2.1 to 5.2.2 ([#16612](https://github.com/opensearch-project/OpenSearch/pull/16612))

### Changed

### Deprecated

### Removed

### Fixed
- Fix get index settings API doesn't show `number_of_routing_shards` setting when it was explicitly set ([#16294](https://github.com/opensearch-project/OpenSearch/pull/16294))
- Revert changes to upload remote state manifest using minimum codec version([#16403](https://github.com/opensearch-project/OpenSearch/pull/16403))
- Ensure index templates are not applied to system indices ([#16418](https://github.com/opensearch-project/OpenSearch/pull/16418))
- Remove resource usages object from search response headers ([#16532](https://github.com/opensearch-project/OpenSearch/pull/16532))
- Support retrieving doc values of unsigned long field ([#16543](https://github.com/opensearch-project/OpenSearch/pull/16543))
- Fix rollover alias supports restored searchable snapshot index([#16483](https://github.com/opensearch-project/OpenSearch/pull/16483))
- Fix permissions error on scripted query against remote snapshot ([#16544](https://github.com/opensearch-project/OpenSearch/pull/16544))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.18...2.x
