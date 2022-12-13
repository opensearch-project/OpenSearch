# CHANGELOG

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Hardened token permissions in GitHub workflows ([#4587](https://github.com/opensearch-project/OpenSearch/pull/4587))
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
- Added missing javadocs for `:example-plugins` modules ([#4540](https://github.com/opensearch-project/OpenSearch/pull/4540))
- Add missing Javadoc tag descriptions in unit tests ([#4629](https://github.com/opensearch-project/OpenSearch/pull/4629))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Apply reproducible builds configuration for OpenSearch plugins through gradle plugin ([#4746](https://github.com/opensearch-project/OpenSearch/pull/4746))
- Add project health badges to the README.md ([#4843](https://github.com/opensearch-project/OpenSearch/pull/4843))
- [Test] Add IAE test for deprecated edgeNGram analyzer name ([#5040](https://github.com/opensearch-project/OpenSearch/pull/5040))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add feature flag for extensions ([#5211](https://github.com/opensearch-project/OpenSearch/pull/5211))

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
- Bumps `commons-compress` from 1.21 to 1.22
- Bumps `jcodings` from 1.0.57 to 1.0.58 ([#5233](https://github.com/opensearch-project/OpenSearch/pull/5233))
- Bumps `google-http-client-jackson2` from 1.35.0 to 1.42.3 ([#5234](https://github.com/opensearch-project/OpenSearch/pull/5234))
- Bumps `maxmind-db` from 2.0.0 to 2.1.0 ([#5236](https://github.com/opensearch-project/OpenSearch/pull/5236))
- Bumps `azure-core` from 1.33.0 to 1.34.0 ([#5235](https://github.com/opensearch-project/OpenSearch/pull/5235))
- Bumps `azure-core-http-netty` from 1.12.4 to 1.12.7 ([#5235](https://github.com/opensearch-project/OpenSearch/pull/5235))
- Bumps `spock-core` from from 2.1-groovy-3.0 to 2.3-groovy-3.0 ([#5315](https://github.com/opensearch-project/OpenSearch/pull/5315))
- Bumps `json-schema-validator` from 1.0.69 to 1.0.73 ([#5316](https://github.com/opensearch-project/OpenSearch/pull/5316))
- Bumps `proto-google-common-protos` from 2.8.0 to 2.10.0 ([#5318](https://github.com/opensearch-project/OpenSearch/pull/5318))
- Bumps `protobuf-java` from 3.21.7 to 3.21.9 ([#5319](https://github.com/opensearch-project/OpenSearch/pull/5319))
- Update Apache Lucene to 9.5.0-snapshot-a4ef70f ([#4979](https://github.com/opensearch-project/OpenSearch/pull/4979))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Support remote translog transfer for request level durability([#4480](https://github.com/opensearch-project/OpenSearch/pull/4480))
- Changed http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Change http code for DecommissioningFailedException from 500 to 400 ([#5283](https://github.com/opensearch-project/OpenSearch/pull/5283))

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
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fixed compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Reject bulk requests with invalid actions ([#5299](https://github.com/opensearch-project/OpenSearch/issues/5299))
- Support OpenSSL Provider with default Netty allocator ([#5460](https://github.com/opensearch-project/OpenSearch/pull/5460))

### Security

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
 - Added transport actions support for extensions ([#4598](https://github.com/opensearch-project/OpenSearch/pull/4598/))
 - Pass REST params and content to extensions ([#4633](https://github.com/opensearch-project/OpenSearch/pull/4633))
 - Return consumed params and content from extensions ([#4705](https://github.com/opensearch-project/OpenSearch/pull/4705))
 - Modified EnvironmentSettingsRequest to pass entire Settings object ([#4731](https://github.com/opensearch-project/OpenSearch/pull/4731))
 - Added contentParser method to ExtensionRestRequest ([#4760](https://github.com/opensearch-project/OpenSearch/pull/4760))
 - Enforce type safety for RegisterTransportActionsRequest([#4796](https://github.com/opensearch-project/OpenSearch/pull/4796))
 - Modified local node request to return Discovery Node ([#4862](https://github.com/opensearch-project/OpenSearch/pull/4862))
 - Enforce type safety for NamedWriteableRegistryParseRequest ([#4923](https://github.com/opensearch-project/OpenSearch/pull/4923))
 - Renaming to match merge to main branch ([5362](https://github.com/opensearch-project/OpenSearch/pull/5362))
 - Updated settings registration changes to reflect main ([5532](https://github.com/opensearch-project/OpenSearch/pull/5532))
 - Added dependency information to Extensions ([#5438](https://github.com/opensearch-project/OpenSearch/pull/5438))

## [Unreleased 2.x]
### Added
- Prevent deletion of snapshots that are backing searchable snapshot indexes ([#5069](https://github.com/opensearch-project/OpenSearch/pull/5069))
- Add max_shard_size parameter for shrink API ([#5229](https://github.com/opensearch-project/OpenSearch/pull/5229))

### Dependencies
- Bumps `bcpg-fips` from 1.0.5.1 to 1.0.7.1
- Bumps `azure-storage-blob` from 12.16.1 to 12.20.0 ([#4995](https://github.com/opensearch-project/OpenSearch/pull/4995))
- Bumps `commons-compress` from 1.21 to 1.22 ([#5104](https://github.com/opensearch-project/OpenSearch/pull/5104))
- Bump `opencensus-contrib-http-util` from 0.18.0 to 0.31.1 ([#3633](https://github.com/opensearch-project/OpenSearch/pull/3633))
- Bump `geoip2` from 3.0.1 to 3.0.2 ([#5103](https://github.com/opensearch-project/OpenSearch/pull/5103))
- Bump gradle-extra-configurations-plugin from 7.0.0 to 8.0.0 ([#4808](https://github.com/opensearch-project/OpenSearch/pull/4808))
### Changed
### Deprecated
### Removed
### Fixed
- Fix 1.x compatibility bug with stored Tasks ([#5412](https://github.com/opensearch-project/OpenSearch/pull/5412))
- Fix case sensitivity for wildcard queries ([#5462](https://github.com/opensearch-project/OpenSearch/pull/5462))
### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.4...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.4...2.x
