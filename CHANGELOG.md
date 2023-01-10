
# CHANGELOG

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
- Add feature flag for extensions ([#5211](https://github.com/opensearch-project/OpenSearch/pull/5211))
### Added
- Add support of default replica count cluster setting ([#5610](https://github.com/opensearch-project/OpenSearch/pull/5610))
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5069](https://github.com/opensearch-project/OpenSearch/pull/5069))
- Update to Gradle 7.6 ([#5382](https://github.com/opensearch-project/OpenSearch/pull/5382))
- Reject bulk requests with invalid actions ([#5299](https://github.com/opensearch-project/OpenSearch/issues/5299))
- Add max_shard_size parameter for shrink API ([#5229](https://github.com/opensearch-project/OpenSearch/pull/5229))
- Added jackson dependency to server ([#5366] (https://github.com/opensearch-project/OpenSearch/pull/5366))
- Adding support to register settings dynamically ([#5495](https://github.com/opensearch-project/OpenSearch/pull/5495))
- Adding auto release workflow ([#5582](https://github.com/opensearch-project/OpenSearch/pull/5582))
- Added experimental support for extensions ([#5347](https://github.com/opensearch-project/OpenSearch/pull/5347)), ([#5518](https://github.com/opensearch-project/OpenSearch/pull/5518)), ([#5597](https://github.com/opensearch-project/OpenSearch/pull/5597)), ([#5615](https://github.com/opensearch-project/OpenSearch/pull/5615)))
- Add CI bundle pattern to distribution download ([#5348](https://github.com/opensearch-project/OpenSearch/pull/5348))
- Added @gbbafna as an OpenSearch maintainer ([#5668](https://github.com/opensearch-project/OpenSearch/pull/5668))
- Experimental support for extended backward compatiblity in searchable snapshots ([#5429](https://github.com/opensearch-project/OpenSearch/pull/5429))
- Added Request level Durability using Remote Translog functionality ([#5757](https://github.com/opensearch-project/OpenSearch/pull/5757))
- Added new level to get health per awareness attribute in _cluster/health ([#5694](https://github.com/opensearch-project/OpenSearch/pull/5694))

### Dependencies
- Bump bcpg-fips from 1.0.5.1 to 1.0.7.1 ([#5148](https://github.com/opensearch-project/OpenSearch/pull/5148))
- Bumps `commons-compress` from 1.21 to 1.22 ([#5104](https://github.com/opensearch-project/OpenSearch/pull/5104))
- Bumps `gson` from 2.9.0 to 2.10 ([#5184](https://github.com/opensearch-project/OpenSearch/pull/5184))
- Bump `gradle-extra-configurations-plugin` from 7.0.0 to 8.0.0 ([#4808](https://github.com/opensearch-project/OpenSearch/pull/4808))
- Bumps `jcodings` from 1.0.57 to 1.0.58
- Bumps `google-http-client-jackson2` from 1.35.0 to 1.42.3
- Bumps `azure-core` from 1.33.0 to 1.34.0
- Bumps `azure-core-http-netty` from 1.12.4 to 1.12.7
- Bumps `maxmind-db` from 2.0.0 to 2.1.0
- Bumps `json-schema-validator` from 1.0.69 to 1.0.73 ([#5316](https://github.com/opensearch-project/OpenSearch/pull/5316))
- Bumps `proto-google-common-protos` from 2.8.0 to 2.10.0 ([#5318](https://github.com/opensearch-project/OpenSearch/pull/5318))
- Bumps `protobuf-java` from 3.21.7 to 3.21.9 ([#5319](https://github.com/opensearch-project/OpenSearch/pull/5319))
- Bumps `gradle-info-plugin` from 7.1.3 to 12.0.0 ([#5684](https://github.com/opensearch-project/OpenSearch/pull/5684))
- Bumps `reactor-netty` from 1.0.18 to 1.1.1 ([#5685](https://github.com/opensearch-project/OpenSearch/pull/5685))
- Bumps `apache-rat` from 0.13 to 0.15 ([#5686](https://github.com/opensearch-project/OpenSearch/pull/5686))
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))

### Changed
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Pre conditions check before updating weighted routing metadata ([#4955](https://github.com/opensearch-project/OpenSearch/pull/4955))
- Integrate remote segment store in the failover flow ([#5579](https://github.com/opensearch-project/OpenSearch/pull/5579))

### Deprecated
- Refactor fuzziness interface on query builders ([#5433](https://github.com/opensearch-project/OpenSearch/pull/5433))

### Removed
### Fixed
- Fix 1.x compatibility bug with stored Tasks ([#5412](https://github.com/opensearch-project/OpenSearch/pull/5412))
- Fix case sensitivity for wildcard queries ([#5462](https://github.com/opensearch-project/OpenSearch/pull/5462))
- Support OpenSSL Provider with default Netty allocator ([#5499](https://github.com/opensearch-project/OpenSearch/pull/5499))
- Apply cluster manager throttling settings during bootstrap ([#5524](https://github.com/opensearch-project/OpenSearch/pull/5524))
- Update thresholds map when cluster manager throttling setting is removed ([#5524](https://github.com/opensearch-project/OpenSearch/pull/5524))
- Fix backward compatibility for static cluster manager throttling threshold setting ([#5633](https://github.com/opensearch-project/OpenSearch/pull/5633))
- Fix index exclusion behavior in snapshot restore and clone APIs ([#5626](https://github.com/opensearch-project/OpenSearch/pull/5626))
- Fix graph filter error in search ([#5665](https://github.com/opensearch-project/OpenSearch/pull/5665))
- [Test] Renaming PIT tests to IT to fix intermittent test failures ([#5750](https://github.com/opensearch-project/OpenSearch/pull/5750))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.4...2.x
