## 2023-01-10 Version 2.5.0 Release Notes

## [2.5]
### Added
- Added jackson dependency to server ([#5366](https://github.com/opensearch-project/OpenSearch/pull/5366))
- Added experimental support for extensions ([#5347](https://github.com/opensearch-project/OpenSearch/pull/5347)), ([#5518](https://github.com/opensearch-project/OpenSearch/pull/5518)), ([#5597](https://github.com/opensearch-project/OpenSearch/pull/5597)), ([#5615](https://github.com/opensearch-project/OpenSearch/pull/5615)))
- Adding support to register settings dynamically ([#5495](https://github.com/opensearch-project/OpenSearch/pull/5495))
- Adding auto release workflow ([#5582](https://github.com/opensearch-project/OpenSearch/pull/5582))
- Add CI bundle pattern to distribution download ([#5348](https://github.com/opensearch-project/OpenSearch/pull/5348))
- Experimental support for extended backward compatiblity in searchable snapshots ([#5429](https://github.com/opensearch-project/OpenSearch/pull/5429))
- Add support of default replica count cluster setting ([#5610](https://github.com/opensearch-project/OpenSearch/pull/5610))
- Add support for refresh level durability ([#5253](https://github.com/opensearch-project/OpenSearch/pull/5253))
- Added Request level Durability using Remote Translog functionality ([#5757](https://github.com/opensearch-project/OpenSearch/pull/5757))
- Support to fail open requests on search shard failures with weighted traffic routing ([#5072](https://github.com/opensearch-project/OpenSearch/pull/5072))
- Support versioning for Weighted routing apis([#5255](https://github.com/opensearch-project/OpenSearch/pull/5255))
- Add support for discovered cluster manager and remove local weights ([#5680](https://github.com/opensearch-project/OpenSearch/pull/5680))
- Added new level to get health per awareness attribute in _cluster/health ([#5694](https://github.com/opensearch-project/OpenSearch/pull/5694))

### Changed
- Pre conditions check before updating weighted routing metadata ([#4955](https://github.com/opensearch-project/OpenSearch/pull/4955))
- Support remote translog transfer for request level durability([#4480](https://github.com/opensearch-project/OpenSearch/pull/4480))
- Gracefully handle concurrent zone decommission action ([#5542](https://github.com/opensearch-project/OpenSearch/pull/5542))

### Deprecated
- Refactor fuzziness interface on query builders ([#5433](https://github.com/opensearch-project/OpenSearch/pull/5433))

### Fixed
- Apply cluster manager throttling settings during bootstrap ([#5524](https://github.com/opensearch-project/OpenSearch/pull/5524))
- Update thresholds map when cluster manager throttling setting is removed ([#5524](https://github.com/opensearch-project/OpenSearch/pull/5524))
- Fix backward compatibility for static cluster manager throttling threshold setting ([#5633](https://github.com/opensearch-project/OpenSearch/pull/5633))
- Fix index exclusion behavior in snapshot restore and clone APIs ([#5626](https://github.com/opensearch-project/OpenSearch/pull/5626))
- Fix graph filter error in search ([#5665](https://github.com/opensearch-project/OpenSearch/pull/5665))
- [Test] Renaming PIT tests to IT to fix intermittent test failures ([#5750](https://github.com/opensearch-project/OpenSearch/pull/5750))

### Dependencies
- Bumps `apache-rat` from 0.13 to 0.15 ([#5686](https://github.com/opensearch-project/OpenSearch/pull/5686))
- Bumps `reactor-netty` from 1.0.18 to 1.1.1 ([#5685](https://github.com/opensearch-project/OpenSearch/pull/5685))
- Bumps `gradle-info-plugin` from 7.1.3 to 12.0.0 ([#5684](https://github.com/opensearch-project/OpenSearch/pull/5684))
