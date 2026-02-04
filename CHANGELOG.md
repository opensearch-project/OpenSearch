# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add getWrappedScorer method to ProfileScorer for plugin access to wrapped scorers ([#20548](https://github.com/opensearch-project/OpenSearch/issues/20548))
- Support expected cluster name with validation in CCS Sniff mode ([#20532](https://github.com/opensearch-project/OpenSearch/pull/20532))

### Changed

### Fixed
- Fix flaky test failures in ShardsLimitAllocationDeciderIT ([#20375](https://github.com/opensearch-project/OpenSearch/pull/20375))
- Prevent criteria update for context aware indices ([#20250](https://github.com/opensearch-project/OpenSearch/pull/20250))
- Update EncryptedBlobContainer to adhere limits while listing blobs in specific sort order if wrapped blob container supports ([#20514](https://github.com/opensearch-project/OpenSearch/pull/20514))

### Dependencies
- Bump `ch.qos.logback:logback-core` and `ch.qos.logback:logback-classic` from 1.5.24 to 1.5.27 ([#20525](https://github.com/opensearch-project/OpenSearch/pull/20525))

### Removed

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.6...main
