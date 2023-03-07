# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add GeoTile and GeoHash Grid aggregations on GeoShapes. ([#5589](https://github.com/opensearch-project/OpenSearch/pull/5589))
- Disallow multiple data paths for search nodes ([#6427](https://github.com/opensearch-project/OpenSearch/pull/6427))
- [Segment Replication] Allocation and rebalancing based on average primary shard count per index ([#6422](https://github.com/opensearch-project/OpenSearch/pull/6422))

### Dependencies
- Bump `org.apache.logging.log4j:log4j-core` from 2.18.0 to 2.20.0 ([#6490](https://github.com/opensearch-project/OpenSearch/pull/6490))
- Bump `com.azure:azure-storage-common` from 12.19.3 to 12.20.0 ([#6492](https://github.com/opensearch-project/OpenSearch/pull/6492)
- Bump `snakeyaml` from 1.33 to 2.0 ([#6511](https://github.com/opensearch-project/OpenSearch/pull/6511))
- Bump `io.projectreactor.netty:reactor-netty` from 1.1.3 to 1.1.4

### Changed
- Require MediaType in Strings.toString API ([#6009](https://github.com/opensearch-project/OpenSearch/pull/6009))
- [Refactor] XContent base classes from xcontent to core library ([#5902](https://github.com/opensearch-project/OpenSearch/pull/5902))

### Deprecated

### Removed

### Fixed
- Added depth check in doc parser for deep nested document ([#5199](https://github.com/opensearch-project/OpenSearch/pull/5199))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.5...2.x
