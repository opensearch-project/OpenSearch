# CHANGELOG

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

### Added

- Add support for s390x architecture ([#4001](https://github.com/opensearch-project/OpenSearch/pull/4001))
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Point in time rest layer changes for create and delete PIT API ([#4064](https://github.com/opensearch-project/OpenSearch/pull/4064))
- Point in time rest layer changes for list PIT and PIT segments API ([#4388](https://github.com/opensearch-project/OpenSearch/pull/4388))
- Added @dreamer-89 as an Opensearch maintainer ([#4342](https://github.com/opensearch-project/OpenSearch/pull/4342))
- Added release notes for 1.3.5 ([#4343](https://github.com/opensearch-project/OpenSearch/pull/4343))
- Added release notes for 2.2.1 ([#4344](https://github.com/opensearch-project/OpenSearch/pull/4344))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- BWC version 2.2.2 ([#4383](https://github.com/opensearch-project/OpenSearch/pull/4383))
- Support for labels on version bump PRs, skip label support for changelog verifier ([#4391](https://github.com/opensearch-project/OpenSearch/pull/4391))
- Update previous release bwc version to 2.4.0 ([#4455](https://github.com/opensearch-project/OpenSearch/pull/4455))
- 2.3.0 release notes ([#4457](https://github.com/opensearch-project/OpenSearch/pull/4457))
- Added missing javadocs for `:distribution:tools` modules ([#4483](https://github.com/opensearch-project/OpenSearch/pull/4483))
- Add BWC version 2.3.1 ([#4513](https://github.com/opensearch-project/OpenSearch/pull/4513))
- [Segment Replication] Add snapshot and restore tests for segment replication feature ([#3993](https://github.com/opensearch-project/OpenSearch/pull/3993))
- Added missing javadocs for `:example-plugins` modules ([#4540](https://github.com/opensearch-project/OpenSearch/pull/4540))
- Add missing Javadoc tag descriptions in unit tests ([#4629](https://github.com/opensearch-project/OpenSearch/pull/4629))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Added precommit support for windows ([#4676](https://github.com/opensearch-project/OpenSearch/pull/4676))
- Added release notes for 1.3.6 ([#4681](https://github.com/opensearch-project/OpenSearch/pull/4681))
- Added precommit support for MacOS ([#4682](https://github.com/opensearch-project/OpenSearch/pull/4682))
- Recommission API changes for service layer ([#4320](https://github.com/opensearch-project/OpenSearch/pull/4320))
### Dependencies
- Bumps `log4j-core` from 2.18.0 to 2.19.0
- Bumps `reactor-netty-http` from 1.0.18 to 1.0.23
- Bumps `jettison` from 1.5.0 to 1.5.1
- Bumps `azure-storage-common` from 12.18.0 to 12.18.1
- Bumps `forbiddenapis` from 3.3 to 3.4


### Dependencies
- Bumps `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bumps `azure-core-http-netty` from 1.12.0 to 1.12.4 ([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps `azure-core` from 1.27.0 to 1.31.0 ([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps `azure-storage-common` from 12.16.0 to 12.18.0 ([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps `org.gradle.test-retry` from 1.4.0 to 1.4.1 ([#4411](https://github.com/opensearch-project/OpenSearch/pull/4411))
- Bumps `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bumps `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bumps `com.diffplug.spotless` from 6.10.0 to 6.11.0 ([#4547](https://github.com/opensearch-project/OpenSearch/pull/4547))
- Bumps `reactor-core` from 3.4.18 to 3.4.23 ([#4548](https://github.com/opensearch-project/OpenSearch/pull/4548))
- Bumps `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Bumps `hadoop-hdfs` from 3.3.3 to 3.3.4 ([#4644](https://github.com/opensearch-project/OpenSearch/pull/4644))
- Bumps `jna` from 5.11.0 to 5.12.1 ([#4656](https://github.com/opensearch-project/OpenSearch/pull/4656))

### Changed

- Dependency updates (httpcore, mockito, slf4j, httpasyncclient, commons-codec) ([#4308](https://github.com/opensearch-project/OpenSearch/pull/4308))
- Use RemoteSegmentStoreDirectory instead of RemoteDirectory ([#4240](https://github.com/opensearch-project/OpenSearch/pull/4240))
- Plugin ZIP publication groupId value is configurable ([#4156](https://github.com/opensearch-project/OpenSearch/pull/4156))
- Weighted round-robin scheduling policy for shard coordination traffic ([#4241](https://github.com/opensearch-project/OpenSearch/pull/4241))
- Add index specific setting for remote repository ([#4253](https://github.com/opensearch-project/OpenSearch/pull/4253))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on SIS files from primary shards. ([#4402](https://github.com/opensearch-project/OpenSearch/pull/4402))
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- [Remote Store] Change behaviour in replica recovery for remote translog enabled indices ([#4318](https://github.com/opensearch-project/OpenSearch/pull/4318))
- PUT api for weighted shard routing ([#4272](https://github.com/opensearch-project/OpenSearch/pull/4272))
- GET api for weighted shard routing([#4275](https://github.com/opensearch-project/OpenSearch/pull/4275/))
- Unmute test RelocationIT.testRelocationWhileIndexingRandom ([#4580](https://github.com/opensearch-project/OpenSearch/pull/4580))
- Add DecommissionService and helper to execute awareness attribute decommissioning ([#4084](https://github.com/opensearch-project/OpenSearch/pull/4084))
- Further simplification of the ZIP publication implementation ([#4360](https://github.com/opensearch-project/OpenSearch/pull/4360))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Load the deprecated master role in a dedicated method instead of in setAdditionalRoles() ([#4582](https://github.com/opensearch-project/OpenSearch/pull/4582))
- Include Windows OS in Bootstrap initializeNatives() check for definitelyRunningAsRoot() ([#4656](https://github.com/opensearch-project/OpenSearch/pull/4656))
- Add APIs (GET/PUT) to decommission awareness attribute ([#4261](https://github.com/opensearch-project/OpenSearch/pull/4261))
- Improve Gradle pre-commit checks to pre-empt Jenkins build ([#4660](https://github.com/opensearch-project/OpenSearch/pull/4660))
- Update to Apache Lucene 9.4.0 ([#4661](https://github.com/opensearch-project/OpenSearch/pull/4661))
- Controlling discovery for decommissioned nodes ([#4590](https://github.com/opensearch-project/OpenSearch/pull/4590))
- Backport Apache Lucene version change for 2.4.0 ([#4677](https://github.com/opensearch-project/OpenSearch/pull/4677))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))

### Fixed

- `opensearch-service.bat start` and `opensearch-service.bat manager` failing to run ([#4289](https://github.com/opensearch-project/OpenSearch/pull/4289))
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- `opensearch.bat` and `opensearch-service.bat install` failing to run, missing logs directory ([#4305](https://github.com/opensearch-project/OpenSearch/pull/4305))
- Restore using the class ClusterInfoRequest and ClusterInfoRequestBuilder from package 'org.opensearch.action.support.master.info' for subclasses ([#4307](https://github.com/opensearch-project/OpenSearch/pull/4307))
- Do not fail replica shard due to primary closure ([#4133](https://github.com/opensearch-project/OpenSearch/pull/4133))
- Add timeout on Mockito.verify to reduce flakyness in testReplicationOnDone test([#4314](https://github.com/opensearch-project/OpenSearch/pull/4314))
- Commit workflow for dependabot changelog helper ([#4331](https://github.com/opensearch-project/OpenSearch/pull/4331))
- Fixed cancellation of segment replication events ([#4225](https://github.com/opensearch-project/OpenSearch/pull/4225))
- [Segment Replication] Bump segment infos counter before commit during replica promotion ([#4365](https://github.com/opensearch-project/OpenSearch/pull/4365))
- Bugs for dependabot changelog verifier workflow ([#4364](https://github.com/opensearch-project/OpenSearch/pull/4364))
- Fix flaky random test `NRTReplicationEngineTests.testUpdateSegments` ([#4352](https://github.com/opensearch-project/OpenSearch/pull/4352))
- [Segment Replication] Extend FileChunkWriter to allow cancel on transport client ([#4386](https://github.com/opensearch-project/OpenSearch/pull/4386))
- [Segment Replication] Add check to cancel ongoing replication with old primary on onNewCheckpoint on replica ([#4363](https://github.com/opensearch-project/OpenSearch/pull/4363))
- Fix NoSuchFileExceptions with segment replication when computing primary metadata snapshots ([#4366](https://github.com/opensearch-project/OpenSearch/pull/4366))
- [Segment Replication] Update flaky testOnNewCheckpointFromNewPrimaryCancelOngoingReplication unit test ([#4414](https://github.com/opensearch-project/OpenSearch/pull/4414))
- Fixed the `_cat/shards/10_basic.yml` test cases fix.
- [Segment Replication] Fix timeout issue by calculating time needed to process getSegmentFiles ([#4426](https://github.com/opensearch-project/OpenSearch/pull/4426))
- [Bug]: gradle check failing with java heap OutOfMemoryError ([#4328](https://github.com/opensearch-project/OpenSearch/))
- `opensearch.bat` fails to execute when install path includes spaces ([#4362](https://github.com/opensearch-project/OpenSearch/pull/4362))
- Getting security exception due to access denied 'java.lang.RuntimePermission' 'accessDeclaredMembers' when trying to get snapshot with S3 IRSA ([#4469](https://github.com/opensearch-project/OpenSearch/pull/4469))
- Fixed flaky test `ResourceAwareTasksTests.testTaskIdPersistsInThreadContext` ([#4484](https://github.com/opensearch-project/OpenSearch/pull/4484))
- Fixed the ignore_malformed setting to also ignore objects ([#4494](https://github.com/opensearch-project/OpenSearch/pull/4494))
- [Segment Replication] Ignore lock file when testing cleanupAndPreserveLatestCommitPoint ([#4544](https://github.com/opensearch-project/OpenSearch/pull/4544))
- Updated jackson to 2.13.4 and snakeyml to 1.32 ([#4556](https://github.com/opensearch-project/OpenSearch/pull/4556))
- Fixing PIT flaky tests ([4632](https://github.com/opensearch-project/OpenSearch/pull/4632))
- Fixed day of year defaulting for round up parser ([#4627](https://github.com/opensearch-project/OpenSearch/pull/4627))
- Fixed the SnapshotsInProgress error during index deletion ([#4570](https://github.com/opensearch-project/OpenSearch/pull/4570))
- [Segment Replication] Adding check to make sure checkpoint is not processed when a shard's shard routing is primary ([#4630](https://github.com/opensearch-project/OpenSearch/pull/4630))
- [Bug]: Fixed invalid location of JDK dependency for arm64 architecture([#4613](https://github.com/opensearch-project/OpenSearch/pull/4613))
- [Bug]: Alias filter lost after rollover ([#4499](https://github.com/opensearch-project/OpenSearch/pull/4499))
- Fixed the SnapshotsInProgress error during index deletion ([#4570](https://github.com/opensearch-project/OpenSearch/pull/4570))
- [Segment Replication] Adding check to make sure checkpoint is not processed when a shard's shard routing is primary ([#4630](https://github.com/opensearch-project/OpenSearch/pull/4630))
- [Bug]: Fixed invalid location of JDK dependency for arm64 architecture([#4613](https://github.com/opensearch-project/OpenSearch/pull/4613))
- [Bug]: Alias filter lost after rollover ([#4499](https://github.com/opensearch-project/OpenSearch/pull/4499))
- Fixed misunderstanding message "No OpenSearchException found" when detailed_error disabled ([#4669](https://github.com/opensearch-project/OpenSearch/pull/4669))
- Attempt to fix Github workflow for Gradle Check job ([#4679](https://github.com/opensearch-project/OpenSearch/pull/4679))
- Fix flaky DecommissionControllerTests.testTimesOut ([4683](https://github.com/opensearch-project/OpenSearch/pull/4683))
- Fix new race condition in DecommissionControllerTests ([4688](https://github.com/opensearch-project/OpenSearch/pull/4688))
- Fix SearchStats (de)serialization (caused by https://github.com/opensearch-project/OpenSearch/pull/4616) ([#4697](https://github.com/opensearch-project/OpenSearch/pull/4697))

### Security

- CVE-2022-25857 org.yaml:snakeyaml DOS vulnerability ([#4341](https://github.com/opensearch-project/OpenSearch/pull/4341))

## [2.x]

### Added

- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
- Added RestLayer Changes for PIT stats ([#4217](https://github.com/opensearch-project/OpenSearch/pull/4217))
- Added GeoBounds aggregation on GeoShape field type.([#4266](https://github.com/opensearch-project/OpenSearch/pull/4266))
  - Addition of Doc values on the GeoShape Field
  - Addition of GeoShape ValueSource level code interfaces for accessing the DocValues.
  - Addition of Missing Value feature in the GeoShape Aggregations.

### Changed

### Deprecated

### Removed

### Fixed

- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- Commit workflow for dependabot changelog helper ([#4331](https://github.com/opensearch-project/OpenSearch/pull/4331))

### Security

[Unreleased]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...HEAD
[2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...2.x
