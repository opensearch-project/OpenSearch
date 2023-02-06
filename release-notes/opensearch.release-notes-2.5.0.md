## 2023-01-10 Version 2.5.0 Release Notes

## [2.5]
### Added
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5365](https://github.com/opensearch-project/OpenSearch/pull/5365))
- Reject bulk requests with invalid actions ([#5384](https://github.com/opensearch-project/OpenSearch/issues/5384))
- Add max_shard_size parameter for shrink API ([#5229](https://github.com/opensearch-project/OpenSearch/pull/5229))
- Add experimental support for extensions ([#5347](https://github.com/opensearch-project/OpenSearch/pull/5347)), ([#5518](https://github.com/opensearch-project/OpenSearch/pull/5518)), ([#5597](https://github.com/opensearch-project/OpenSearch/pull/5597)), ([#5615](https://github.com/opensearch-project/OpenSearch/pull/5615)))
- Add support to register settings dynamically ([#5495](https://github.com/opensearch-project/OpenSearch/pull/5495))
- Add auto release workflow ([#5582](https://github.com/opensearch-project/OpenSearch/pull/5582))
- Add CI bundle pattern to distribution download ([#5348](https://github.com/opensearch-project/OpenSearch/pull/5348))
- Experimental support for extended backward compatiblity in searchable snapshots ([#5429](https://github.com/opensearch-project/OpenSearch/pull/5429))
- Add support of default replica count cluster setting ([#5610](https://github.com/opensearch-project/OpenSearch/pull/5610))
- Add support for refresh level durability ([#5253](https://github.com/opensearch-project/OpenSearch/pull/5253))
- Add Request level Durability using Remote Translog functionality ([#5757](https://github.com/opensearch-project/OpenSearch/pull/5757))
- Support to fail open requests on search shard failures with weighted traffic routing ([#5072](https://github.com/opensearch-project/OpenSearch/pull/5072))
- Support versioning for Weighted routing apis([#5255](https://github.com/opensearch-project/OpenSearch/pull/5255))
- Add support for discovered cluster manager and remove local weights ([#5680](https://github.com/opensearch-project/OpenSearch/pull/5680))
- Add new level to get health per awareness attribute in _cluster/health ([#5694](https://github.com/opensearch-project/OpenSearch/pull/5694))

### Changed
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Pre conditions check before updating weighted routing metadata ([#4955](https://github.com/opensearch-project/OpenSearch/pull/4955))
- Support remote translog transfer for request level durability ([#4480](https://github.com/opensearch-project/OpenSearch/pull/4480))
- Gracefully handle concurrent zone decommission action ([#5542](https://github.com/opensearch-project/OpenSearch/pull/5542))

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
- Bumps `bcpg-fips` from 1.0.5.1 to 1.0.7.1 ([#5148](https://github.com/opensearch-project/OpenSearch/pull/5148))
- Bumps `commons-compress` from 1.21 to 1.22 ([#5104](https://github.com/opensearch-project/OpenSearch/pull/5104))
- Bumps `geoip2` from 3.0.1 to 3.0.2 in /modules/ingest-geoip ([#5201](https://github.com/opensearch-project/OpenSearch/pull/5201))
- Bumps `gson` from 2.9.0 to 2.10 in /plugins/repository-hdfs ([#5184](https://github.com/opensearch-project/OpenSearch/pull/5184))
- Bumps `protobuf-java` from 3.21.8 to 3.21.9 in /test/fixtures/hdfs-fixture ([#5185](https://github.com/opensearch-project/OpenSearch/pull/5185))
- Bumps `gradle-extra-configurations-plugin` from 7.0.0 to 8.0.0 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `jcodings` from 1.0.57 to 1.0.58 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `google-http-client-jackson2` from 1.35.0 to 1.42.3 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `azure-core` from 1.33.0 to 1.34.0 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `azure-core-http-netty` from 1.12.4 to 1.12.7 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `maxmind-db` from 2.0.0 to 2.1.0 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `json-schema-validator` from 1.0.69 to 1.0.73 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `proto-google-common-protos` from 2.8.0 to 2.10.0 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `protobuf-java` from 3.21.7 to 3.21.9 ([#5330](https://github.com/opensearch-project/OpenSearch/pull/5330))
- Bumps `gradle` from 7.5 to 7.6 ([#5382](https://github.com/opensearch-project/OpenSearch/pull/5382))
- Bumps `jackson` from 2.14.0 to 2.14.1 ([#5355](https://github.com/opensearch-project/OpenSearch/pull/5355))
- Bumps `apache-rat` from 0.13 to 0.15 ([#5686](https://github.com/opensearch-project/OpenSearch/pull/5686))
- Bumps `reactor-netty` from 1.0.18 to 1.1.1 ([#5685](https://github.com/opensearch-project/OpenSearch/pull/5685))
- Bumps `gradle-info-plugin` from 7.1.3 to 12.0.0 ([#5684](https://github.com/opensearch-project/OpenSearch/pull/5684))
