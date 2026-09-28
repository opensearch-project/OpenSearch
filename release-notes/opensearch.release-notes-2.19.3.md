## 2025-07-22 Version 2.19.3 Release Notes

## [2.19.3]
### Added
- Reject close index requests, while remote store migration is in progress.([#18327](https://github.com/opensearch-project/OpenSearch/pull/18327))

### Dependencies
- Bump `netty` from 4.1.118.Final to 4.1.121.Final ([#18192](https://github.com/opensearch-project/OpenSearch/pull/18192))
- Bump Apache Lucene to 9.12.2 ([#18574](https://github.com/opensearch-project/OpenSearch/pull/18574))
- Bump `commons-beanutils:commons-beanutils` from 1.9.4 to 1.11.0 ([#18401](https://github.com/opensearch-project/OpenSearch/issues/18401))
- Bump `org.apache.poi` version from 5.2.5 to 5.4.1 in /plugins/ingest-attachment ([#17887](https://github.com/opensearch-project/OpenSearch/pull/17887))

### Fixed
- Use Bad Request status for InputCoercionException ([#18161](https://github.com/opensearch-project/OpenSearch/pull/18161))
- Avoid NPE if on SnapshotInfo if 'shallow' boolean not present ([#18187](https://github.com/opensearch-project/OpenSearch/issues/18187))
- Null check field names in QueryStringQueryBuilder ([#18194](https://github.com/opensearch-project/OpenSearch/pull/18194))
- Fix illegal argument exception when creating a PIT ([#16781](https://github.com/opensearch-project/OpenSearch/pull/16781))
- Fix the bug of Access denied error when rolling log files ([#18597](https://github.com/opensearch-project/OpenSearch/pull/18597))

### Changed
- Change single shard assignment log message from warn to debug ([#18186](https://github.com/opensearch-project/OpenSearch/pull/18186))
