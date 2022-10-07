# CHANGELOG
Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
### Added
- Add support for s390x architecture ([#4001](https://github.com/opensearch-project/OpenSearch/pull/4001))
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Point in time rest layer changes for create and delete PIT API ([#4064](https://github.com/opensearch-project/OpenSearch/pull/4064))
- Added @dreamer-89 as an Opensearch maintainer ([#4342](https://github.com/opensearch-project/OpenSearch/pull/4342))
- Added release notes for 1.3.5 ([#4343](https://github.com/opensearch-project/OpenSearch/pull/4343))
- Added release notes for 2.2.1 ([#4344](https://github.com/opensearch-project/OpenSearch/pull/4344))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- BWC version 2.2.2 ([#4383](https://github.com/opensearch-project/OpenSearch/pull/4383))
- Support for labels on version bump PRs, skip label support for changelog verifier ([#4391](https://github.com/opensearch-project/OpenSearch/pull/4391))
- Update previous release bwc version to 2.4.0 ([#4455](https://github.com/opensearch-project/OpenSearch/pull/4455))
- 2.3.0 release notes ([#4457](https://github.com/opensearch-project/OpenSearch/pull/4457))
- Warn future developers not to pass headers to extensions ([#4430](https://github.com/opensearch-project/OpenSearch/pull/4430))
- Added missing javadocs for `:distribution:tools` modules ([#4483](https://github.com/opensearch-project/OpenSearch/pull/4483))
- Add BWC version 2.3.1 ([#4513](https://github.com/opensearch-project/OpenSearch/pull/4513))
- [Segment Replication] Add snapshot and restore tests for segment replication feature ([#3993](https://github.com/opensearch-project/OpenSearch/pull/3993))
- Principal Identity for Opensearch requests to/from extensions ([#4299](https://github.com/opensearch-project/OpenSearch/pull/4299))

### Dependencies
- Bumps `reactive-streams` from 1.0.3 to 1.0.4


### Dependencies
- Bumps `org.gradle.test-retry` from 1.4.0 to 1.4.1
- Bumps `reactor-netty-core` from 1.0.19 to 1.0.22

### Dependencies
- Bumps `com.diffplug.spotless` from 6.9.1 to 6.10.0
- Bumps `xmlbeans` from 5.1.0 to 5.1.1
- Bumps azure-core-http-netty from 1.12.0 to 1.12.4([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps azure-core from 1.27.0 to 1.31.0([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))
- Bumps azure-storage-common from 12.16.0 to 12.18.0([#4160](https://github.com/opensearch-project/OpenSearch/pull/4160))

### Changed
- Dependency updates (httpcore, mockito, slf4j, httpasyncclient, commons-codec) ([#4308](https://github.com/opensearch-project/OpenSearch/pull/4308))
- Use RemoteSegmentStoreDirectory instead of RemoteDirectory ([#4240](https://github.com/opensearch-project/OpenSearch/pull/4240))
- Plugin ZIP publication groupId value is configurable ([#4156](https://github.com/opensearch-project/OpenSearch/pull/4156))
- Add index specific setting for remote repository ([#4253](https://github.com/opensearch-project/OpenSearch/pull/4253))
- [Segment Replication] Update replicas to commit SegmentInfos instead of relying on SIS files from primary shards. ([#4402](https://github.com/opensearch-project/OpenSearch/pull/4402))
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))

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
- [Segment Replication] Bump segment infos counter before commit during replica promotion ([#4365](https://github.com/opensearch-project/OpenSearch/pull/4365))
- Bugs for dependabot changelog verifier workflow ([#4364](https://github.com/opensearch-project/OpenSearch/pull/4364))
- Fix flaky random test `NRTReplicationEngineTests.testUpdateSegments` ([#4352](https://github.com/opensearch-project/OpenSearch/pull/4352))
- [Segment Replication] Extend FileChunkWriter to allow cancel on transport client ([#4386](https://github.com/opensearch-project/OpenSearch/pull/4386))
- [Segment Replication] Add check to cancel ongoing replication with old primary on onNewCheckpoint on replica ([#4363](https://github.com/opensearch-project/OpenSearch/pull/4363))
- Fix NoSuchFileExceptions with segment replication when computing primary metadata snapshots ([#4366](https://github.com/opensearch-project/OpenSearch/pull/4366))
- [Segment Replication] Update flaky testOnNewCheckpointFromNewPrimaryCancelOngoingReplication unit test ([#4414](https://github.com/opensearch-project/OpenSearch/pull/4414))
- Fixed the `_cat/shards/10_basic.yml` test cases fix.
- [Segment Replication] Fix timeout issue by calculating time needed to process getSegmentFiles ([#4426](https://github.com/opensearch-project/OpenSearch/pull/4426))
- [Bug]: gradle check failing with java heap OutOfMemoryError (([#4328](https://github.com/opensearch-project/OpenSearch/
- `opensearch.bat` fails to execute when install path includes spaces ([#4362](https://github.com/opensearch-project/OpenSearch/pull/4362))
- Getting security exception due to access denied 'java.lang.RuntimePermission' 'accessDeclaredMembers' when trying to get snapshot with S3 IRSA ([#4469](https://github.com/opensearch-project/OpenSearch/pull/4469))
- Fixed flaky test `ResourceAwareTasksTests.testTaskIdPersistsInThreadContext` ([#4484](https://github.com/opensearch-project/OpenSearch/pull/4484))
- Fixed the ignore_malformed setting to also ignore objects ([#4494](https://github.com/opensearch-project/OpenSearch/pull/4494))

### Security
- CVE-2022-25857 org.yaml:snakeyaml DOS vulnerability ([#4341](https://github.com/opensearch-project/OpenSearch/pull/4341))

## Feature/extensions Branch

### Added
 - Adding initial support for extensions ([#2796](https://github.com/opensearch-project/OpenSearch/pull/2796))
 - Adding extension framework support for first extension point ([#3107](https://github.com/opensearch-project/OpenSearch/pull/3107))
 - Updated imports with absolute path ([#3173](https://github.com/opensearch-project/OpenSearch/pull/3173))
 - Fix a bug where indexing times out when extensions do not exist ([#3188](https://github.com/opensearch-project/OpenSearch/pull/3188))
 - Integrated CreateComponent extensionPoint ([#3265](https://github.com/opensearch-project/OpenSearch/pull/3265))
 - Added javadocs for extensibility ([#3366](https://github.com/opensearch-project/OpenSearch/pull/3366))
 - Resolved javadoc error ([#3380](https://github.com/opensearch-project/OpenSearch/pull/3380))
 - Read from extensions.yml ([#3381](https://github.com/opensearch-project/OpenSearch/pull/3381))
 - Added unit tests and modified extension read logic for ExtensionsOrchestrator ([#3449](https://github.com/opensearch-project/OpenSearch/pull/3449))
 - Removed Plugin Directory code from ExtensionsOrchestrator ([#3721](https://github.com/opensearch-project/OpenSearch/pull/3721))
 - Added unit test for createComponent workflow ([#3750](https://github.com/opensearch-project/OpenSearch/pull/3750))
 - Adding support to register settings dynamically ([#3753](https://github.com/opensearch-project/OpenSearch/pull/3753))
 - Added flag to check if connectToNode request is coming from ExtensionsOrchestrator ([#3830](https://github.com/opensearch-project/OpenSearch/pull/3830))
 - Extensibility support for getNamedWriteables ([#3925](https://github.com/opensearch-project/OpenSearch/pull/3925))
 - Restructured testExtensionsInitialize ([#4029](https://github.com/opensearch-project/OpenSearch/pull/4029))
 - Provide Extension API to OpenSearch ([#4100](https://github.com/opensearch-project/OpenSearch/pull/4100))
 - Register REST API and forward REST requests to associated extension ([#4282](https://github.com/opensearch-project/OpenSearch/pull/4282))
 - Only send one extension info when initializing ([#4302](https://github.com/opensearch-project/OpenSearch/pull/4302))
 - Adding support for registering Transport Actions for extensions ([#4326](https://github.com/opensearch-project/OpenSearch/pull/4326))
 - Pass full RestResponse to user from Extension ([#4356](https://github.com/opensearch-project/OpenSearch/pull/4356))
 - Handle named wildcards (REST path parameters) ([#4415](https://github.com/opensearch-project/OpenSearch/pull/4415))
 - Adding ActionListener onFailure to ExtensionsOrchestrator ([#4191](https://github.com/opensearch-project/OpenSearch/pull/4191))
 - Created new Request class for ActionListener onFailure ([#4553](https://github.com/opensearch-project/OpenSearch/pull/4553))
 - Adding create component extension point support for AD ([#4517](https://github.com/opensearch-project/OpenSearch/pull/4517))
 - Add getSettings support for AD([#4519](https://github.com/opensearch-project/OpenSearch/pull/4519))
 - Fixed javadoc warning for build failure([#4581](https://github.com/opensearch-project/OpenSearch/pull/4581))
 - Pass REST params and content to extensions ([#4633](https://github.com/opensearch-project/OpenSearch/pull/4633))
 - Return consumed params and content from extensions ([#4705](https://github.com/opensearch-project/OpenSearch/pull/4705))

## [2.x]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
- Added RestLayer Changes for PIT stats ([#4217](https://github.com/opensearch-project/OpenSearch/pull/4217))

### Changed

### Deprecated

### Removed

### Fixed
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- Commit workflow for dependabot changelog helper ([#4331](https://github.com/opensearch-project/OpenSearch/pull/4331))

### Security


[Unreleased]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...HEAD
[2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...2.x
