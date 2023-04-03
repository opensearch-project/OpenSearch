# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add GeoTile and GeoHash Grid aggregations on GeoShapes. ([#5589](https://github.com/opensearch-project/OpenSearch/pull/5589))
- Disallow multiple data paths for search nodes ([#6427](https://github.com/opensearch-project/OpenSearch/pull/6427))
- [Segment Replication] Allocation and rebalancing based on average primary shard count per index ([#6422](https://github.com/opensearch-project/OpenSearch/pull/6422))
- The truncation limit of the OpenSearchJsonLayout logger is now configurable ([#6569](https://github.com/opensearch-project/OpenSearch/pull/6569))
- Add 'base_path' setting to File System Repository ([#6558](https://github.com/opensearch-project/OpenSearch/pull/6558))
- Return success on DeletePits when no PITs exist. ([#6544](https://github.com/opensearch-project/OpenSearch/pull/6544))
- Add node repurpose command for search nodes ([#6517](https://github.com/opensearch-project/OpenSearch/pull/6517))
- Add wait_for_completion parameter to resize, open, and forcemerge APIs ([#6434](https://github.com/opensearch-project/OpenSearch/pull/6434))
- [Segment Replication] Apply backpressure when replicas fall behind ([#6563](https://github.com/opensearch-project/OpenSearch/pull/6563))
- [Remote Store] Integrate remote segment store in peer recovery flow ([#6664](https://github.com/opensearch-project/OpenSearch/pull/6664))
- Enable sort optimization for all NumericTypes ([#6464](https://github.com/opensearch-project/OpenSearch/pull/6464)
- Remove 'cluster_manager' role attachment when using 'node.master' deprecated setting ([#6331](https://github.com/opensearch-project/OpenSearch/pull/6331))
- Add new cluster settings to ignore weighted round-robin routing and fallback to default behaviour. ([#6834](https://github.com/opensearch-project/OpenSearch/pull/6834))
- Add experimental support for ZSTD compression. ([#3577](https://github.com/opensearch-project/OpenSearch/pull/3577))
- [Segment Replication] Add point in time and scroll query compatibility. ([#6644](https://github.com/opensearch-project/OpenSearch/pull/6644))

### Dependencies
- Bump `org.apache.logging.log4j:log4j-core` from 2.18.0 to 2.20.0 ([#6490](https://github.com/opensearch-project/OpenSearch/pull/6490))
- Bump `com.azure:azure-storage-common` from 12.19.3 to 12.20.0 ([#6492](https://github.com/opensearch-project/OpenSearch/pull/6492)
- Bump `snakeyaml` from 1.33 to 2.0 ([#6511](https://github.com/opensearch-project/OpenSearch/pull/6511))
- Bump `io.projectreactor.netty:reactor-netty` from 1.1.3 to 1.1.4
- Bump `com.avast.gradle:gradle-docker-compose-plugin` from 0.15.2 to 0.16.11
- Bump `net.minidev:json-smart` from 2.4.8 to 2.4.9
- Bump `com.google.protobuf:protobuf-java` to 3.22.2 ([#6994](https://github.com/opensearch-project/OpenSearch/pull/6994))
- Bump Netty to 4.1.90.Final ([#6677](https://github.com/opensearch-project/OpenSearch/pull/6677)
- Bump `com.diffplug.spotless` from 6.15.0 to 6.17.0
- Bump `org.apache.zookeeper:zookeeper` from 3.8.0 to 3.8.1
- Bump `net.minidev:json-smart` from 2.4.7 to 2.4.10
- Bump `org.apache.maven:maven-model` from 3.6.2 to 3.9.1
- Bump `org.codehaus.jettison:jettison` from 1.5.3 to 1.5.4 ([#6878](https://github.com/opensearch-project/OpenSearch/pull/6878))
- Add `com.github.luben:zstd-jni:1.5.4-1` ([#3577](https://github.com/opensearch-project/OpenSearch/pull/3577))
- Bump: Netty from 4.1.90.Final to 4.1.91.Final , ASM 9.4 to ASM 9.5, ByteBuddy 1.14.2 to 1.14.3 ([#6981](https://github.com/opensearch-project/OpenSearch/pull/6981))
- Bump `com.azure:azure-storage-blob` from 12.15.0 to 12.21.1

### Changed
- Require MediaType in Strings.toString API ([#6009](https://github.com/opensearch-project/OpenSearch/pull/6009))
- [Refactor] XContent base classes from xcontent to core library ([#5902](https://github.com/opensearch-project/OpenSearch/pull/5902))
- Changed `opensearch-env` to respect already set `OPENSEARCH_HOME` environment variable ([#6956](https://github.com/opensearch-project/OpenSearch/pull/6956/))

### Deprecated
- Map, List, and Set in org.opensearch.common.collect ([#6609](https://github.com/opensearch-project/OpenSearch/pull/6609))

### Removed

### Fixed
- Added depth check in doc parser for deep nested document ([#5199](https://github.com/opensearch-project/OpenSearch/pull/5199))
- Added equals/hashcode for named DocValueFormat.DateTime inner class ([#6357](https://github.com/opensearch-project/OpenSearch/pull/6357))
- Fixed bug for searchable snapshot to take 'base_path' of blob into account ([#6558](https://github.com/opensearch-project/OpenSearch/pull/6558))
- Fix fuzziness validation ([#5805](https://github.com/opensearch-project/OpenSearch/pull/5805))
- Fix GetSnapshots to not return non-existent snapshots with ignore_unavailable=true ([#6839](https://github.com/opensearch-project/OpenSearch/pull/6839))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.5...2.x
