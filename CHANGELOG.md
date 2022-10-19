# CHANGELOG
Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
### Added
- Add support for s390x architecture ([#4001](https://github.com/opensearch-project/OpenSearch/pull/4001))
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Point in time rest layer changes for create and delete PIT API ([#4064](https://github.com/opensearch-project/OpenSearch/pull/4064))
- Add failover support with Segment Replication enabled. ([#4325](https://github.com/opensearch-project/OpenSearch/pull/4325)
- Point in time rest layer changes for list PIT and PIT segments API ([#4388](https://github.com/opensearch-project/OpenSearch/pull/4388))
- Added @dreamer-89 as an Opensearch maintainer ([#4342](https://github.com/opensearch-project/OpenSearch/pull/4342))
- Added release notes for 1.3.5 ([#4343](https://github.com/opensearch-project/OpenSearch/pull/4343))
- Added release notes for 2.2.1 ([#4344](https://github.com/opensearch-project/OpenSearch/pull/4344))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- BWC version 2.2.2 ([#4383](https://github.com/opensearch-project/OpenSearch/pull/4383))
- Support for labels on version bump PRs, skip label support for changelog verifier ([#4391](https://github.com/opensearch-project/OpenSearch/pull/4391))
- Add a new node role 'search' which is dedicated to provide search capability ([#4689](https://github.com/opensearch-project/OpenSearch/pull/4689))
- Introduce experimental searchable snapshot API ([#4680](https://github.com/opensearch-project/OpenSearch/pull/4680))

### Dependencies
- Bumps `com.diffplug.spotless` from 6.9.1 to 6.10.0
- Bumps `xmlbeans` from 5.1.0 to 5.1.1
- Bumps `hadoop-hdfs` from 3.3.3 to 3.3.4
### Changed
- Dependency updates (httpcore, mockito, slf4j, httpasyncclient, commons-codec) ([#4308](https://github.com/opensearch-project/OpenSearch/pull/4308))
- Use RemoteSegmentStoreDirectory instead of RemoteDirectory ([#4240](https://github.com/opensearch-project/OpenSearch/pull/4240))
- Add index specific setting for remote repository ([#4253](https://github.com/opensearch-project/OpenSearch/pull/4253))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on SIS files from primary shards. ([#4402](https://github.com/opensearch-project/OpenSearch/pull/4402))
- Change the version to remove deprecated code of adding node name into log pattern of log4j property file ([#4569](https://github.com/opensearch-project/OpenSearch/pull/4569))
- Update to Apache Lucene 9.4.0 ([#4661](https://github.com/opensearch-project/OpenSearch/pull/4661))
- Load the deprecated master role in a dedicated method instead of in setAdditionalRoles() ([#4582](https://github.com/opensearch-project/OpenSearch/pull/4582))
- Plugin ZIP publication groupId value is configurable ([#4156](https://github.com/opensearch-project/OpenSearch/pull/4156))
- Further simplification of the ZIP publication implementation ([#4360](https://github.com/opensearch-project/OpenSearch/pull/4360))
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
- [Bug]: gradle check failing with java heap OutOfMemoryError ([#4328](https://github.com/opensearch-project/OpenSearch/))
- `opensearch.bat` fails to execute when install path includes spaces ([#4362](https://github.com/opensearch-project/OpenSearch/pull/4362))
- Getting security exception due to access denied 'java.lang.RuntimePermission' 'accessDeclaredMembers' when trying to get snapshot with S3 IRSA ([#4469](https://github.com/opensearch-project/OpenSearch/pull/4469))
- Fixed flaky test `ResourceAwareTasksTests.testTaskIdPersistsInThreadContext` ([#4484](https://github.com/opensearch-project/OpenSearch/pull/4484))
- Fixed the ignore_malformed setting to also ignore objects ([#4494](https://github.com/opensearch-project/OpenSearch/pull/4494))
- Fixed the `_cat/shards/10_basic.yml` test cases fix.
- Updated jackson to 2.13.4 and snakeyml to 1.32 ([#4556](https://github.com/opensearch-project/OpenSearch/pull/4556))
- Fixed day of year defaulting for round up parser ([#4627](https://github.com/opensearch-project/OpenSearch/pull/4627))
- Fixed the SnapshotsInProgress error during index deletion ([#4570](https://github.com/opensearch-project/OpenSearch/pull/4570))
- [Bug]: Fixed invalid location of JDK dependency for arm64 architecture([#4613](https://github.com/opensearch-project/OpenSearch/pull/4613))
- [Bug]: Alias filter lost after rollover ([#4499](https://github.com/opensearch-project/OpenSearch/pull/4499))
- Fixing Gradle warnings associated with publishPluginZipPublicationToXxx tasks ([#4696](https://github.com/opensearch-project/OpenSearch/pull/4696))
- Fixed randomly failing test ([4774](https://github.com/opensearch-project/OpenSearch/pull/4774))

### Security
- CVE-2022-25857 org.yaml:snakeyaml DOS vulnerability ([#4341](https://github.com/opensearch-project/OpenSearch/pull/4341))
## [2.x]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Add timing data and more granular stages to SegmentReplicationState ([#4367](https://github.com/opensearch-project/OpenSearch/pull/4367))
- BWC version 2.2.2 ([#4385](https://github.com/opensearch-project/OpenSearch/pull/4385))
- Added RestLayer Changes for PIT stats ([#4217](https://github.com/opensearch-project/OpenSearch/pull/4217))
- BWC version 1.3.6 ([#4452](https://github.com/opensearch-project/OpenSearch/pull/4452))
- Bump current version to 2.4.0 on 2.x branch ([#4454](https://github.com/opensearch-project/OpenSearch/pull/4454))
- 2.3.0 release notes ([#4457](https://github.com/opensearch-project/OpenSearch/pull/4457))
- Add BWC version 2.3.1 ([#4512](https://github.com/opensearch-project/OpenSearch/pull/4512))
- Updated jackson to 2.13.4 and snakeyml to 1.32 ([#4563](https://github.com/opensearch-project/OpenSearch/pull/4563))
- Add BWC version 1.3.7 ([#4709](https://github.com/opensearch-project/OpenSearch/pull/4709))
- Bump `jettison` from 1.4.1 to 1.5.1 ([#4717](https://github.com/opensearch-project/OpenSearch/pull/4717))
- Set analyzer to regex query string search ([4219](https://github.com/opensearch-project/OpenSearch/pull/4219))
### Dependencies
- Update Jackson Databind to 2.13.4.2 (addressing CVE-2022-42003) ([#4781](https://github.com/opensearch-project/OpenSearch/pull/4781))
- Install and configure Log4j JUL Adapter for Lucene 9.4 ([#4754](https://github.com/opensearch-project/OpenSearch/pull/4754))
### Changed
### Deprecated
### Removed
- Remove RepositoryData.MIN_VERSION support for next major release ([4729](https://github.com/opensearch-project/OpenSearch/pull/4729))
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
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on segments_N from primary shards ([#4450](https://github.com/opensearch-project/OpenSearch/pull/4450))
- [Segment Replication] Adding check to make sure checkpoint is not processed when a shard's shard routing is primary ([#4716](https://github.com/opensearch-project/OpenSearch/pull/4716))
- Disable merge on refresh in DiskThresholdDeciderIT ([#4828](https://github.com/opensearch-project/OpenSearch/pull/4828))

### Security

[Unreleased]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...HEAD
[2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...2.x
