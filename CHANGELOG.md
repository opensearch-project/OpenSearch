# CHANGELOG

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Add project health badges to the README.md ([#4843](https://github.com/opensearch-project/OpenSearch/pull/4843))
- [Test] Add IAE test for deprecated edgeNGram analyzer name ([#5040](https://github.com/opensearch-project/OpenSearch/pull/5040))
- [Identity] Document identity roadmap and feature branch processes ([#4583](https://github.com/opensearch-project/OpenSearch/pull/4583))
- [Identity] Add stubs for AccessTokenManager  ([#4612](https://github.com/opensearch-project/OpenSearch/pull/4612))
- [Identity] Permissions check API ([#4516](https://github.com/opensearch-project/OpenSearch/pull/4516))
- [Identity] Add detail to identity roadmap items ([#4641](https://github.com/opensearch-project/OpenSearch/pull/4641))
- [Identity] Switch to smaller license header for all identity files ([#4657](https://github.com/opensearch-project/OpenSearch/pull/4657))
- [Identity] Include scrawfor99 to the identity team ([#4658](https://github.com/opensearch-project/OpenSearch/pull/4658))
- [Identity] Prototype Internal IdP ([#4659](https://github.com/opensearch-project/OpenSearch/pull/4659))
- [Identity] Strategy for Delegated Authority using Tokens ([#4826](https://github.com/opensearch-project/OpenSearch/pull/4826))
- [Identity] Adds Basic Auth mechanism via Internal IdP ([#4798](https://github.com/opensearch-project/OpenSearch/pull/4798))


### Dependencies
- Bumps `log4j-core` from 2.18.0 to 2.19.0
- Bumps `reactor-netty-http` from 1.0.18 to 1.0.23
- Bumps `jettison` from 1.5.0 to 1.5.1
- Bumps `azure-storage-common` from 12.18.0 to 12.18.1
- Bumps `forbiddenapis` from 3.3 to 3.4
- Bumps `gson` from 2.9.0 to 2.10
- Bumps `protobuf-java` from 3.21.2 to 3.21.9
- Bumps `azure-core` from 1.31.0 to 1.33.0
- Bumps `avro` from 1.11.0 to 1.11.1
- Bumps `woodstox-core` from 6.3.0 to 6.3.1
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
- Update Apache Lucene to 9.5.0-snapshot-a4ef70f ([#4979](https://github.com/opensearch-project/OpenSearch/pull/4979))
- Bumps `bcpg-fips` from 1.0.5.1 to 1.0.7.1
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))

### Changed
<<<<<<< HEAD

- Dependency updates (httpcore, mockito, slf4j, httpasyncclient, commons-codec) ([#4308](https://github.com/opensearch-project/OpenSearch/pull/4308))
- Use RemoteSegmentStoreDirectory instead of RemoteDirectory ([#4240](https://github.com/opensearch-project/OpenSearch/pull/4240))
- Plugin ZIP publication groupId value is configurable ([#4156](https://github.com/opensearch-project/OpenSearch/pull/4156))
- Weighted round-robin scheduling policy for shard coordination traffic ([#4241](https://github.com/opensearch-project/OpenSearch/pull/4241))
- Add index specific setting for remote repository ([#4253](https://github.com/opensearch-project/OpenSearch/pull/4253))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on SIS files from primary shards. ([#4402](https://github.com/opensearch-project/OpenSearch/pull/4402))
=======
>>>>>>> origin/main
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))

### Fixed
<<<<<<< HEAD

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
- Fixing Gradle warnings associated with publishPluginZipPublicationToXxx tasks ([#4696](https://github.com/opensearch-project/OpenSearch/pull/4696))
- [BUG]: Remove redundant field from GetDecommissionStateResponse ([#4751](https://github.com/opensearch-project/OpenSearch/pull/4751))
- Fixed randomly failing test ([4774](https://github.com/opensearch-project/OpenSearch/pull/4774))
- Update version check after backport ([4786](https://github.com/opensearch-project/OpenSearch/pull/4786))
- Fix decommission status update to non leader nodes ([4800](https://github.com/opensearch-project/OpenSearch/pull/4800))
- Fix recovery path for searchable snapshots ([4813](https://github.com/opensearch-project/OpenSearch/pull/4813))
- Fix bug in AwarenessAttributeDecommissionIT([4822](https://github.com/opensearch-project/OpenSearch/pull/4822))
=======
>>>>>>> origin/main
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fixed compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))

### Security
<<<<<<< HEAD

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
- Install and configure Log4j JUL Adapter for Lucene 9.4 ([#4754](https://github.com/opensearch-project/OpenSearch/pull/4754))
- Added feature to ignore indexes starting with dot during shard limit validation.([#4695](https://github.com/opensearch-project/OpenSearch/pull/4695))
=======

## [Unreleased 2.x]
### Added
### Dependencies
>>>>>>> origin/main
### Changed
### Deprecated
### Removed
### Fixed
<<<<<<< HEAD

- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- Commit workflow for dependabot changelog helper ([#4331](https://github.com/opensearch-project/OpenSearch/pull/4331))
- Better plural stemmer than minimal_english ([#4738](https://github.com/opensearch-project/OpenSearch/pull/4738))
- Disable merge on refresh in DiskThresholdDeciderIT ([#4828](https://github.com/opensearch-project/OpenSearch/pull/4828))

=======
>>>>>>> origin/main
### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.4...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.4...2.x
