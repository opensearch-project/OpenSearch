# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add support for restoring from snapshot with search replicas ([#16111](https://github.com/opensearch-project/OpenSearch/pull/16111))
- Switch from `buildSrc/version.properties` to Gradle version catalog (`gradle/libs.versions.toml`) to enable dependabot to perform automated upgrades on common libs ([#16284](https://github.com/opensearch-project/OpenSearch/pull/16284))
- Add dynamic setting allowing size > 0 requests to be cached in the request cache ([#16483](https://github.com/opensearch-project/OpenSearch/pull/16483/files))

### Dependencies
- Bump `com.google.apis:google-api-services-compute` from v1-rev20240407-2.0.0 to v1-rev20241015-2.0.0 ([#16502](https://github.com/opensearch-project/OpenSearch/pull/16502))
- Bump `com.azure:azure-storage-common` from 12.25.1 to 12.27.1 ([#16521](https://github.com/opensearch-project/OpenSearch/pull/16521))
- Bump `com.azure:azure-storage-blob` from 12.23.0 to 12.28.1 ([#16501](https://github.com/opensearch-project/OpenSearch/pull/16501))

### Changed

### Deprecated

### Removed

### Fixed
- Fix get index settings API doesn't show `number_of_routing_shards` setting when it was explicitly set ([#16294](https://github.com/opensearch-project/OpenSearch/pull/16294))
- Revert changes to upload remote state manifest using minimum codec version([#16403](https://github.com/opensearch-project/OpenSearch/pull/16403))
- Ensure index templates are not applied to system indices ([#16418](https://github.com/opensearch-project/OpenSearch/pull/16418))
- Remove resource usages object from search response headers ([#16532](https://github.com/opensearch-project/OpenSearch/pull/16532))
- Support retrieving doc values of unsigned long field ([#16543](https://github.com/opensearch-project/OpenSearch/pull/16543))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.18...2.x
