## 2023-05-18 Version 1.3.10 Release Notes

### Upgrades
- Upgrade Netty from 4.1.90.Final to 4.1.91.Final , ASM 9.4 to ASM 9.5, ByteBuddy 1.14.2 to 1.14.3 ([#6981](https://github.com/opensearch-project/OpenSearch/pull/6981))
- Upgrade `org.gradle.test-retry` from 1.5.1 to 1.5.2
- Upgrade `org.apache.hadoop:hadoop-minicluster` from 3.3.4 to 3.3.5
- OpenJDK Update (April 2023 Patch releases) [#7449](https://github.com/opensearch-project/OpenSearch/pull/7449)
- Upgrade `net.minidev:json-smart` from 2.4.7 to 2.4.10

### Bug Fixes
- Avoid negative memory result in IndicesQueryCache stats calculation ([#6917](https://github.com/opensearch-project/OpenSearch/pull/6917))
