# CHANGELOG

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
- Add feature flag for extensions ([#5211](https://github.com/opensearch-project/OpenSearch/pull/5211))
### Added
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5069](https://github.com/opensearch-project/OpenSearch/pull/5069))
- Update to Gradle 7.6 ([#5382](https://github.com/opensearch-project/OpenSearch/pull/5382))
- Reject bulk requests with invalid actions ([#5299](https://github.com/opensearch-project/OpenSearch/issues/5299))
- Add max_shard_size parameter for shrink API ([#5229](https://github.com/opensearch-project/OpenSearch/pull/5229))
- Added jackson dependency to server ([#5366] (https://github.com/opensearch-project/OpenSearch/pull/5366))
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
### Changed
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))
- Pre conditions check before updating weighted routing metadata ([#4955](https://github.com/opensearch-project/OpenSearch/pull/4955))

### Deprecated
### Removed
### Fixed
- Fix 1.x compatibility bug with stored Tasks ([#5412](https://github.com/opensearch-project/OpenSearch/pull/5412))
- Fix case sensitivity for wildcard queries ([#5462](https://github.com/opensearch-project/OpenSearch/pull/5462))
- Support OpenSSL Provider with default Netty allocator ([#5499](https://github.com/opensearch-project/OpenSearch/pull/5499))
### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.4...2.x
