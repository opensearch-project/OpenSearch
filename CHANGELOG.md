# CHANGELOG
Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Add failover support with Segment Replication enabled. ([#4325](https://github.com/opensearch-project/OpenSearch/pull/4325)
- Added @dreamer-89 as an Opensearch maintainer ([#4342](https://github.com/opensearch-project/OpenSearch/pull/4342))
- Bump version to 2.3.1 and add 2.3.0 to BWC versions ([#4511](https://github.com/opensearch-project/OpenSearch/pull/4511))
- Add BWC version 1.3.7 ([#4711](https://github.com/opensearch-project/OpenSearch/pull/4711))
- Bumps `jettison` from 1.5.0 to 1.5.1 ([#4712](https://github.com/opensearch-project/OpenSearch/pull/4712))

### Changed
- Dependency updates (httpcore, mockito, slf4j, httpasyncclient, commons-codec) ([#4308](https://github.com/opensearch-project/OpenSearch/pull/4308))
- Use RemoteSegmentStoreDirectory instead of RemoteDirectory ([#4240](https://github.com/opensearch-project/OpenSearch/pull/4240))
- Add index specific setting for remote repository ([#4253](https://github.com/opensearch-project/OpenSearch/pull/4253))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on SIS files from primary shards. ([#4402](https://github.com/opensearch-project/OpenSearch/pull/4402))
- Change the version to remove deprecated code of adding node name into log pattern of log4j property file ([#4569](https://github.com/opensearch-project/OpenSearch/pull/4569))
- Dependency updates (jackson, snakeyaml) ([#4622](https://github.com/opensearch-project/OpenSearch/pull/4622)

### Deprecated

### Removed

### Fixed
- `opensearch-service.bat start` and `opensearch-service.bat manager` failing to run ([#4289](https://github.com/opensearch-project/OpenSearch/pull/4289))
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- `opensearch.bat` and `opensearch-service.bat install` failing to run, missing logs directory ([#4305](https://github.com/opensearch-project/OpenSearch/pull/4305))
- Restore using the class ClusterInfoRequest and ClusterInfoRequestBuilder from package 'org.opensearch.action.support.master.info' for subclasses ([#4307](https://github.com/opensearch-project/OpenSearch/pull/4307))
- Do not fail replica shard due to primary closure ([#4133](https://github.com/opensearch-project/OpenSearch/pull/4133))
- Add timeout on Mockito.verify to reduce flakyness in testReplicationOnDone test([#4314](https://github.com/opensearch-project/OpenSearch/pull/4314))
- Commit workflow for dependabot changelog helper ([#4331](https://github.com/opensearch-project/OpenSearch/pull/4331))
- Fixed cancellation of segment replication events ([#4225](https://github.com/opensearch-project/OpenSearch/pull/4225))
- Bugs for dependabot changelog verifier workflow ([#4364](https://github.com/opensearch-project/OpenSearch/pull/4364))
- [Bug]: gradle check failing with java heap OutOfMemoryError (([#4328](https://github.com/opensearch-project/OpenSearch/
- `opensearch.bat` fails to execute when install path includes spaces ([#4362](https://github.com/opensearch-project/OpenSearch/pull/4362))
- Getting security exception due to access denied 'java.lang.RuntimePermission' 'accessDeclaredMembers' when trying to get snapshot with S3 IRSA ([#4469](https://github.com/opensearch-project/OpenSearch/pull/4469))
- Compatibility issue with /_mget: RHLC 2.x connected to OpenSearch Cluster 1.x ([#4812](https://github.com/opensearch-project/OpenSearch/pull/4812))

### Security
- CVE-2022-25857 org.yaml:snakeyaml DOS vulnerability ([#4341](https://github.com/opensearch-project/OpenSearch/pull/4341))

## [2.x]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Add timing data and more granular stages to SegmentReplicationState ([#4367](https://github.com/opensearch-project/OpenSearch/pull/4367))
- BWC version 2.2.2 ([#4385](https://github.com/opensearch-project/OpenSearch/pull/4385))
- BWC version 1.3.6 ([#4452](https://github.com/opensearch-project/OpenSearch/pull/4452))
- 2.3.0 release notes ([#4457](https://github.com/opensearch-project/OpenSearch/pull/4457))

### Dependencies
- Update Jackson Databind to 2.13.4.2 (addressing CVE-2022-42003) ([#4781](https://github.com/opensearch-project/OpenSearch/pull/4781))

### Changed

### Deprecated

### Removed

### Fixed
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- Restore using the class ClusterInfoRequest and ClusterInfoRequestBuilder from package 'org.opensearch.action.support.master.info' for subclasses ([#4324](https://github.com/opensearch-project/OpenSearch/pull/4324))
- Fixed cancellation of segment replication events ([#4225](https://github.com/opensearch-project/OpenSearch/pull/4225))
- [Segment Replication] Add check to cancel ongoing replication with old primary on onNewCheckpoint on replica ([#4363](https://github.com/opensearch-project/OpenSearch/pull/4363))
- [Segment Replication] Bump segment infos counter before commit during replica promotion ([#4365](https://github.com/opensearch-project/OpenSearch/pull/4365))
- [Segment Replication] Update flaky testOnNewCheckpointFromNewPrimaryCancelOngoingReplication unit test ([#4414](https://github.com/opensearch-project/OpenSearch/pull/4414))
- [Segment Replication] Extend FileChunkWriter to allow cancel on transport client ([#4386](https://github.com/opensearch-project/OpenSearch/pull/4386))
- [Segment Replication] Fix NoSuchFileExceptions with segment replication when computing primary metadata snapshots ([#4366](https://github.com/opensearch-project/OpenSearch/pull/4366))
- [Segment Replication] Fix timeout issue by calculating time needed to process getSegmentFiles ([#4434](https://github.com/opensearch-project/OpenSearch/pull/4434))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on segments_N from primary shards.

### Security


[Unreleased]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...HEAD
[2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...2.x
