## Version 2.19.6 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.6

### Bug Fixes
* Fix hang in bulk request when index is deleted during primary phase ([#21305](https://github.com/opensearch-project/OpenSearch/pull/21305))
* Fix case insensitive and escaped query on wildcard fields ([#20870](https://github.com/opensearch-project/OpenSearch/pull/20870))
* Fix array index out of bounds exception with wildcard fields and aggregations ([#20862](https://github.com/opensearch-project/OpenSearch/pull/20862))
* Harden circuit breaker and failure handling logic to prevent negative estimated limits in query result consumer ([#20769](https://github.com/opensearch-project/OpenSearch/pull/20769))
* Prevent negative fielddata stats by guarding against stale removals after shard reallocation ([#22016](https://github.com/opensearch-project/OpenSearch/pull/22016))

### Maintenance
* Update Netty to 4.1.135.Final ([#21968](https://github.com/opensearch-project/OpenSearch/pull/21968))
* Bump org.apache.avro:avro to 1.12.1 ([#22350](https://github.com/opensearch-project/OpenSearch/pull/22350))
* Bump Bouncy Castle (bcprov/bcpkix-jdk18on) to 1.84 ([#22296](https://github.com/opensearch-project/OpenSearch/pull/22296))
* Bump ZooKeeper to 3.9.5 ([#22296](https://github.com/opensearch-project/OpenSearch/pull/22296))
* Bump plexus-utils to 3.6.1 ([#22296](https://github.com/opensearch-project/OpenSearch/pull/22296))
* Bump log4j to 2.25.4 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump Jetty to 9.4.58 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump maven-model to 3.9.16 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump plexus-xml to 3.0.1 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump commons-configuration2 to 2.15.0 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump jsoup to 1.22.2 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump Jackson to 2.18.8 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump reactor-netty to 1.2.18 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Bump reactor-core to 3.7.19 ([#22292](https://github.com/opensearch-project/OpenSearch/pull/22292))
* Update bundled JDK to JDK 21.0.11+10 ([#21419](https://github.com/opensearch-project/OpenSearch/pull/21419))
