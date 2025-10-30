## Version 2.19.4 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.4

### Added
* New cluster setting search.query.max_query_string_length ([#19491](https://github.com/opensearch-project/OpenSearch/pull/19491))

### Dependencies
* Bump Apache Lucene to 9.12.3 ([#19444](https://github.com/opensearch-project/OpenSearch/pull/19444))
* Bump `org.bouncycastle:bc-fips` from 2.0.0 to 2.1.2 ([#19155](https://github.com/opensearch-project/OpenSearch/pull/19155))
* Bump `org.apache.commons:commons-lang3` from 3.14.0 to 3.18.0 ([#19155](https://github.com/opensearch-project/OpenSearch/pull/19155))
* Bump `org.bouncycastle:bcprov-jdk18on` from 1.78 to 1.79 ([#19155](https://github.com/opensearch-project/OpenSearch/pull/19155))
* Bump `org.bouncycastle:bcmail-jdk18on` from 1.78 to 1.79 ([#19155](https://github.com/opensearch-project/OpenSearch/pull/19155))
* Bump `org.bouncycastle:bcpkix-jdk18on` from 1.78 to 1.79 ([#19155](https://github.com/opensearch-project/OpenSearch/pull/19155))
* Bump `org.apache.tika` from 2.9.2 to 3.2.2 ([#19242](https://github.com/opensearch-project/OpenSearch/pull/19242))
* Bump `org.apache.commons:commons-compress` from 1.26.1 to 1.28.0 ([#19125](https://github.com/opensearch-project/OpenSearch/pull/19242))
* Bump `org.apache.commons:commonscodec` from 1.16.1 to 1.18.0 ([#19125](https://github.com/opensearch-project/OpenSearch/pull/19242))
* Replace commons-lang:commons-lang with org.apache.commons:commons-lang3 ([#19229](https://github.com/opensearch-project/OpenSearch/pull/19229))
* Bump netty from 4.1.121.Final to 4.1.125.Final ([#19270](https://github.com/opensearch-project/OpenSearch/pull/19270))
* Bump bouncycastle from 1.79 to 1.82 ([#19552](https://github.com/opensearch-project/OpenSearch/pull/19552))
* Bump `org.ajoberstar.grgit:grgit-core` from 5.2.1 to 5.3.2 ([#19606](https://github.com/opensearch-project/OpenSearch/pull/19606))
* Bump `reactor-netty` from 1.1.23 to 1.2.9 ([#19603](https://github.com/opensearch-project/OpenSearch/pull/19603))
* Bump `reactor` from 3.5.20 to 3.7.5 ([#19603](https://github.com/opensearch-project/OpenSearch/pull/19603))
* Bump `org.apache.hadoop:hadoop-minicluster` from 3.4.1 to 3.4.2 ([#19605](https://github.com/opensearch-project/OpenSearch/pull/19605))
* Bump `io.grpc` deps from 1.68.2 to 1.75.0 ([#19495](https://github.com/opensearch-project/OpenSearch/pull/19495))
* Bump `com.nimbusds:nimbus-jose-jwt` from 10.0.2 to 10.3 ([#19604](https://github.com/opensearch-project/OpenSearch/pull/19604))
* Exclude commons-lang and org.jsonschema2pojo from hadoop-miniclusters ([#19538](https://github.com/opensearch-project/OpenSearch/pull/19538))

### Fixed
* Add task cancellation checks in aggregators ([#18426](https://github.com/opensearch-project/OpenSearch/pull/18426))
* Fix OOM due to large number of shard result buffering ([#19066](https://github.com/opensearch-project/OpenSearch/pull/19066))
* Fix QueryPhaseResultConsumer incomplete callback loops ([#19231](https://github.com/opensearch-project/OpenSearch/pull/19231))
* Use ScoreDoc instead of FieldDoc when creating TopScoreDocCollectorManager to avoid unnecessary conversion ([#18802](https://github.com/opensearch-project/OpenSearch/pull/18802))
* Fix IndexOutOfBoundsException when running include/exclude on non-existent prefix in terms aggregations ([#19637](https://github.com/opensearch-project/OpenSearch/pull/19637))

### Changed
* Replace centos:8 with almalinux:8 since centos docker images are deprecated ([#19154](https://github.com/opensearch-project/OpenSearch/pull/19154))
* Allow plugins to copy folders into their config dir during installation ([#19343](https://github.com/opensearch-project/OpenSearch/pull/19343))
* Onboarding new maven snapshots publishing to s3 ([#19632](https://github.com/opensearch-project/OpenSearch/pull/19632))