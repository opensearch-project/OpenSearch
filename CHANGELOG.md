# CHANGELOG
Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Added @dreamer-89 as an Opensearch maintainer ([#4342](https://github.com/opensearch-project/OpenSearch/pull/4342))
- Added release notes for 1.3.5 ([#4343](https://github.com/opensearch-project/OpenSearch/pull/4343))
- Added release notes for 2.2.1 ([#4344](https://github.com/opensearch-project/OpenSearch/pull/4344))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- BWC version 2.2.2 ([#4383](https://github.com/opensearch-project/OpenSearch/pull/4383))
- Support for labels on version bump PRs, skip label support for changelog verifier ([#4391](https://github.com/opensearch-project/OpenSearch/pull/4391))
- Copy `build.sh` over from opensearch-build ([#4887](https://github.com/opensearch-project/OpenSearch/pull/4887))

### Dependencies
- Bumps `com.diffplug.spotless` from 6.9.1 to 6.10.0
- Bumps `xmlbeans` from 5.1.0 to 5.1.1
- Bumps `commons-configuration2` from 2.7 to 2.8
- Exclude jettison version brought in with hadoop-minicluster. ([#4787](https://github.com/opensearch-project/OpenSearch/pull/4787))
- Updating bundled JDK to 11.0.16.1+1 and 8u345-b01 ([#4888](https://github.com/opensearch-project/OpenSearch/pull/4888))
- Bump protobuf-java to 3.21.7 in repository-gcs and repository-hdfs ([#4890](https://github.com/opensearch-project/OpenSearch/pull/4890))
- Upgrade netty to 4.1.84.Final ([#4893](https://github.com/opensearch-project/OpenSearch/pull/4893))
- Bump reactor-netty-http to 1.0.24 in repository-azure ([#4920](https://github.com/opensearch-project/OpenSearch/pull/4920))
- Bump `tika` from 2.4.0 to 2.5.0 ([#4929](https://github.com/opensearch-project/OpenSearch/pull/4929))
- Bump `woodstox-core` to 6.4.0 ([#4950](https://github.com/opensearch-project/OpenSearch/pull/4950))
- Upgrade jetty-http, kotlin-stdlib and snakeyaml ([#4981](https://github.com/opensearch-project/OpenSearch/pull/4981))
- OpenJDK Update (October 2022 Patch releases) ([#4998](https://github.com/opensearch-project/OpenSearch/pull/4998))
- Update Jackson to 2.14.0 ([#5105](https://github.com/opensearch-project/OpenSearch/pull/5105))
- OpenJDK Update (January 2023 Patch releases) ([#6077](https://github.com/opensearch-project/OpenSearch/pull/6077))

### Changed
- Dependency updates (httpcore, mockito, slf4j, httpasyncclient, commons-codec) ([#4308](https://github.com/opensearch-project/OpenSearch/pull/4308))
- Use RemoteSegmentStoreDirectory instead of RemoteDirectory ([#4240](https://github.com/opensearch-project/OpenSearch/pull/4240))
- Plugin ZIP publication groupId value is configurable ([#4156](https://github.com/opensearch-project/OpenSearch/pull/4156))
- Skip SymbolicLinkPreservingTarIT when running on Windows ([#5023](https://github.com/opensearch-project/OpenSearch/pull/5023))

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
- `opensearch.bat` fails to execute when install path includes spaces ([#4362](https://github.com/opensearch-project/OpenSearch/pull/4362))
- Fix error handling while reading analyzer mapping rules ((6d20423)[https://github.com/opensearch-project/OpenSearch/commit/6d20423f5920745463b1abc5f1daf6a786c41aa0])
- Fix for failing checkExtraction, checkLicense and checkNotice tasks for windows gradle check ([#4941](https://github.com/opensearch-project/OpenSearch/pull/4941))

### Security
- CVE-2022-25857 org.yaml:snakeyaml DOS vulnerability ([#4341](https://github.com/opensearch-project/OpenSearch/pull/4341))
## [2.x]
### Added
- Github workflow for changelog verification ([#4085](https://github.com/opensearch-project/OpenSearch/pull/4085))
- Label configuration for dependabot PRs ([#4348](https://github.com/opensearch-project/OpenSearch/pull/4348))
### Changed
### Deprecated
### Removed
### Fixed
- `opensearch-service.bat start` and `opensearch-service.bat manager` failing to run ([#4289](https://github.com/opensearch-project/OpenSearch/pull/4289))
- PR reference to checkout code for changelog verifier ([#4296](https://github.com/opensearch-project/OpenSearch/pull/4296))
- `opensearch.bat` and `opensearch-service.bat install` failing to run, missing logs directory ([#4305](https://github.com/opensearch-project/OpenSearch/pull/4305))
- Fix graph filter error in search ([#5665](https://github.com/opensearch-project/OpenSearch/pull/5665))
### Security
## [1.x]
### Added
- Backported CODEOWNERS file and Dependabot configuration
- Bump version to 1.3.7 ([#4701](https://github.com/opensearch-project/OpenSearch/pull/4701))
### Dependencies
- Bumps jackson to 2.13.4 and snakeyml to 1.32 ([#4599](https://github.com/opensearch-project/OpenSearch/pull/4599))
- Update Jackson Databind to 2.13.4.2 (addressing CVE-2022-42003) ([#4782](https://github.com/opensearch-project/OpenSearch/pull/4782))
- Bump protobuf-java to 3.21.8 ([#5006](https://github.com/opensearch-project/OpenSearch/pull/5006))
- Upgrade zookeeper dependency in hdfs-fixture ([#5046](https://github.com/opensearch-project/OpenSearch/pull/5046))

[Unreleased]: https://github.com/opensearch-project/OpenSearch/compare/2.2.0...HEAD
