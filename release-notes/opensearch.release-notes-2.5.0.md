## 2023-01-10 Version 2.5.0 Release Notes

## [2.5]
### Added
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5069](https://github.com/opensearch-project/OpenSearch/pull/5069))
- Add max_shard_size parameter for shrink API ([#5229](https://github.com/opensearch-project/OpenSearch/pull/5229))
- Added support to apply index create block ([#4603](https://github.com/opensearch-project/OpenSearch/issues/4603))
- Support request level durability for remote-backed indexes ([#5671](https://github.com/opensearch-project/OpenSearch/issues/5671))
- Added new level to get health per awareness attribute in _cluster/health ([#5694](https://github.com/opensearch-project/OpenSearch/pull/5694))

### Changed
- Add support for refresh level durability ([#5253](https://github.com/opensearch-project/OpenSearch/pull/5253))
- Integrate remote segment store in the failover flow ([#5579](https://github.com/opensearch-project/OpenSearch/pull/5579))

### Deprecated
- Refactor fuzziness interface on query builders ([#5433](https://github.com/opensearch-project/OpenSearch/pull/5433))

### Fixed
- Fix case sensitivity for wildcard queries ([#5462](https://github.com/opensearch-project/OpenSearch/pull/5462))
- Apply cluster manager throttling settings during bootstrap ([#5524](https://github.com/opensearch-project/OpenSearch/pull/5524))
- Update thresholds map when cluster manager throttling setting is removed ([#5524](https://github.com/opensearch-project/OpenSearch/pull/5524))
- Fix backward compatibility for static cluster manager throttling threshold setting ([#5633](https://github.com/opensearch-project/OpenSearch/pull/5633))
- Fix index exclusion behavior in snapshot restore and clone APIs ([#5626](https://github.com/opensearch-project/OpenSearch/pull/5626))
- Fix graph filter error in search ([#5665](https://github.com/opensearch-project/OpenSearch/pull/5665))

### Dependencies
- Bump `bcpg-fips` from 1.0.5.1 to 1.0.7.1
- Bump `azure-storage-blob` from 12.16.1 to 12.20.0 ([#4995](https://github.com/opensearch-project/OpenSearch/pull/4995))
- Bump `commons-compress` from 1.21 to 1.22 ([#5104](https://github.com/opensearch-project/OpenSearch/pull/5104))
- Bump `opencensus-contrib-http-util` from 0.18.0 to 0.31.1 ([#3633](https://github.com/opensearch-project/OpenSearch/pull/3633))
- Bump `geoip2` from 3.0.2 to 4.0.0 ([#5634](https://github.com/opensearch-project/OpenSearch/pull/5634))
- Bump gradle-extra-configurations-plugin from 7.0.0 to 8.0.0 ([#4808](https://github.com/opensearch-project/OpenSearch/pull/4808))
- Bump `gradle-info-plugin` from 11.3.3 to 12.0.0 ([#5600](https://github.com/opensearch-project/OpenSearch/pull/5600))
- Bump `apache-rat` from 0.13 to 0.15 ([#5675](https://github.com/opensearch-project/OpenSearch/pull/5675))
- Bump `reactor-netty` from 1.0.18 to 1.1.1 ([#5676](https://github.com/opensearch-project/OpenSearch/pull/5676))
