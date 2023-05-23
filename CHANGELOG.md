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
- [Segment Replication] Add new cluster setting to set replication strategy by default for all indices in cluster. ([#6791](https://github.com/opensearch-project/OpenSearch/pull/6791))
- Enable sort optimization for all NumericTypes ([#6464](https://github.com/opensearch-project/OpenSearch/pull/6464)
- Remove 'cluster_manager' role attachment when using 'node.master' deprecated setting ([#6331](https://github.com/opensearch-project/OpenSearch/pull/6331))

### Dependencies
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.0.0 to 12.1.3 (#7564)
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.2.0
- Bump `com.google.protobuf:protobuf-java` from 3.22.2 to 3.22.3
- Bump `jackson` from 2.14.2 to 2.15.0 ([#7286](https://github.com/opensearch-project/OpenSearch/pull/7286))
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 20.2.0 to 20.3.0
- Bump `com.netflix.nebula.ospackage-base` from 11.0.0 to 11.3.0
- Bump `gradle.plugin.com.github.johnrengelman:shadow` from 7.1.2 to 8.0.0
- Bump `jna` from 5.11.0 to 5.13.0
- Bump `commons-io:commons-io` from 2.7 to 2.12.0 (#7661, #7658, #7656)
- Bump `org.apache.shiro:shiro-core` from 1.9.1 to 1.11.0 ([#7397](https://github.com/opensearch-project/OpenSearch/pull/7397))
- Bump `jetty-server` in hdfs-fixture from 9.4.49.v20220914 to 9.4.51.v20230217 ([#7405](https://github.com/opensearch-project/OpenSearch/pull/7405))
- OpenJDK Update (April 2023 Patch releases) ([#7448](https://github.com/opensearch-project/OpenSearch/pull/7448)
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0 (#7462)
- Bump `com.azure:azure-core` from 1.34.0 to 1.39.0
- Bump `com.networknt:json-schema-validator` from 1.0.78 to 1.0.81 (#7460)
- Bump Apache Lucene to 9.6.0 ([#7505](https://github.com/opensearch-project/OpenSearch/pull/7505))
- Bump `com.google.cloud:google-cloud-core-http` from 1.93.3 to 2.17.0 (#7488)
- Bump `com.google.guava:guava` from 30.1.1-jre to 31.1-jre (#7565)
- Bump `com.azure:azure-storage-common` from 12.20.0 to 12.21.0 (#7566)
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0 (#7563)
- Bump `jackson` from 2.15.0 to 2.15.1 ([#7603](https://github.com/opensearch-project/OpenSearch/pull/7603))
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660)
- Bump `io.projectreactor.netty:reactor-netty-core` from 1.1.5 to 1.1.7 (#7657)
- Bump `org.apache.maven:maven-model` from 3.9.1 to 3.9.2 (#7655)
- Bump `com.google.api:gax` from 2.17.0 to 2.27.0 (#7697)

### Changed
- Require MediaType in Strings.toString API ([#6009](https://github.com/opensearch-project/OpenSearch/pull/6009))
- [Refactor] XContent base classes from xcontent to core library ([#5902](https://github.com/opensearch-project/OpenSearch/pull/5902))

### Deprecated
- Map, List, and Set in org.opensearch.common.collect ([#6609](https://github.com/opensearch-project/OpenSearch/pull/6609))

### Removed

### Fixed
- Added depth check in doc parser for deep nested document ([#5199](https://github.com/opensearch-project/OpenSearch/pull/5199))
- Added equals/hashcode for named DocValueFormat.DateTime inner class ([#6357](https://github.com/opensearch-project/OpenSearch/pull/6357))
- Fixed bug for searchable snapshot to take 'base_path' of blob into account ([#6558](https://github.com/opensearch-project/OpenSearch/pull/6558))
- Fix fuzziness validation ([#5805](https://github.com/opensearch-project/OpenSearch/pull/5805))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.7...2.x
