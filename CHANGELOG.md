# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add fingerprint ingest processor ([#13724](https://github.com/opensearch-project/OpenSearch/pull/13724))
- [Remote Store] Rate limiter for remote store low priority uploads ([#14374](https://github.com/opensearch-project/OpenSearch/pull/14374/))
- Apply the date histogram rewrite optimization to range aggregation ([#13865](https://github.com/opensearch-project/OpenSearch/pull/13865))
- [Writable Warm] Add composite directory implementation and integrate it with FileCache ([12782](https://github.com/opensearch-project/OpenSearch/pull/12782))
- [Workload Management] Add QueryGroup schema ([13669](https://github.com/opensearch-project/OpenSearch/pull/13669))
- Add batching supported processor base type AbstractBatchingProcessor ([#14554](https://github.com/opensearch-project/OpenSearch/pull/14554))
- Fix race condition while parsing derived fields from search definition ([14445](https://github.com/opensearch-project/OpenSearch/pull/14445))
- Add `strict_allow_templates` dynamic mapping option ([#14555](https://github.com/opensearch-project/OpenSearch/pull/14555))
- Add allowlist setting for ingest-common and search-pipeline-common processors ([#14439](https://github.com/opensearch-project/OpenSearch/issues/14439))
- Create SystemIndexRegistry with helper method matchesSystemIndex ([#14415](https://github.com/opensearch-project/OpenSearch/pull/14415))
- Print reason why parent task was cancelled ([#14604](https://github.com/opensearch-project/OpenSearch/issues/14604))
- [Range Queries] Add new approximateable query framework to short-circuit range queries ([#13788](https://github.com/opensearch-project/OpenSearch/pull/13788))

### Dependencies
- Bump `org.apache.commons:commons-configuration2` from 2.10.0 to 2.10.1 ([#12896](https://github.com/opensearch-project/OpenSearch/pull/12896))
- Bump `asm` from 9.6 to 9.7 ([#12908](https://github.com/opensearch-project/OpenSearch/pull/12908))
- Bump `net.minidev:json-smart` from 2.5.0 to 2.5.1 ([#12893](https://github.com/opensearch-project/OpenSearch/pull/12893), [#13117](https://github.com/opensearch-project/OpenSearch/pull/13117))
- Bump `netty` from 4.1.107.Final to 4.1.109.Final ([#12924](https://github.com/opensearch-project/OpenSearch/pull/12924), [#13233](https://github.com/opensearch-project/OpenSearch/pull/13233))
- Bump `commons-io:commons-io` from 2.15.1 to 2.16.0 ([#12996](https://github.com/opensearch-project/OpenSearch/pull/12996), [#12998](https://github.com/opensearch-project/OpenSearch/pull/12998), [#12999](https://github.com/opensearch-project/OpenSearch/pull/12999))
- Bump `org.apache.commons:commons-compress` from 1.24.0 to 1.26.1 ([#12627](https://github.com/opensearch-project/OpenSearch/pull/12627))
- Bump `org.apache.commons:commonscodec` from 1.15 to 1.16.1 ([#12627](https://github.com/opensearch-project/OpenSearch/pull/12627))
- Bump `org.apache.commons:commonslang` from 3.13.0 to 3.14.0 ([#12627](https://github.com/opensearch-project/OpenSearch/pull/12627))
- Bump Apache Tika from 2.6.0 to 2.9.2 ([#12627](https://github.com/opensearch-project/OpenSearch/pull/12627))
- Bump `com.gradle.enterprise` from 3.16.2 to 3.17.2 ([#13116](https://github.com/opensearch-project/OpenSearch/pull/13116), [#13191](https://github.com/opensearch-project/OpenSearch/pull/13191), [#13327](https://github.com/opensearch-project/OpenSearch/pull/13327))
- Bump `gradle/wrapper-validation-action` from 2 to 3 ([#13192](https://github.com/opensearch-project/OpenSearch/pull/13192))
- Bump joda from 2.12.2 to 2.12.7 ([#13193](https://github.com/opensearch-project/OpenSearch/pull/13193))
- Bump bouncycastle from 1.77 to 1.78 ([#13243](https://github.com/opensearch-project/OpenSearch/pull/13243))
- Update google dependencies in repository-gcs and discovery-gce ([#13213](https://github.com/opensearch-project/OpenSearch/pull/13213))
- Bump `com.google.apis:google-api-services-compute` from v1-rev235-1.25.0 to v1-rev20240407-2.0.0 ([#13333](https://github.com/opensearch-project/OpenSearch/pull/13333))
- Bump `commons-cli:commons-cli` from 1.6.0 to 1.7.0 ([#13331](https://github.com/opensearch-project/OpenSearch/pull/13331))
- Bump `com.github.spullara.mustache.java:compiler` from 0.9.10 to 0.9.11 ([#13329](https://github.com/opensearch-project/OpenSearch/pull/13329))
- Bump `jakarta.enterprise:jakarta.enterprise.cdi-api` from 4.0.1 to 4.1.0 ([#13328](https://github.com/opensearch-project/OpenSearch/pull/13328))
- Bump `com.google.api.grpc:proto-google-iam-v1` from 0.12.0 to 1.33.0 ([#13332](https://github.com/opensearch-project/OpenSearch/pull/13332))
- OpenJDK Update (April 2024 Patch releases), update to Eclipse Temurin 21.0.3+9 ([#13389](https://github.com/opensearch-project/OpenSearch/pull/13389))
- Bump `com.squareup.okio:okio` from 3.8.0 to 3.9.0 ([#12997](https://github.com/opensearch-project/OpenSearch/pull/12997))
- Bump `com.netflix.nebula.ospackage-base` from 11.8.1 to 11.9.0 ([#13440](https://github.com/opensearch-project/OpenSearch/pull/13440))
- Bump `org.bouncycastle:bc-fips` from 1.0.2.4 to 1.0.2.5 ([#13446](https://github.com/opensearch-project/OpenSearch/pull/13446))
- Bump `lycheeverse/lychee-action` from 1.9.3 to 1.10.0 ([#13447](https://github.com/opensearch-project/OpenSearch/pull/13447))
- Bump `org.gradle.test-retry` from 1.5.8 to 1.5.9 ([#13442](https://github.com/opensearch-project/OpenSearch/pull/13442))

### Changed
- [BWC and API enforcement] Enforcing the presence of API annotations at build time ([#12872](https://github.com/opensearch-project/OpenSearch/pull/12872))
- Improve built-in secure transports support ([#12907](https://github.com/opensearch-project/OpenSearch/pull/12907))
- Update links to documentation in rest-api-spec ([#13043](https://github.com/opensearch-project/OpenSearch/pull/13043))
- Ignoring unavailable shards during search request execution with ignore_available parameter ([#13298](https://github.com/opensearch-project/OpenSearch/pull/13298))
- Refactoring globMatch using simpleMatchWithNormalizedStrings from Regex ([#13104](https://github.com/opensearch-project/OpenSearch/pull/13104))
- [BWC and API enforcement] Reconsider the breaking changes check policy to detect breaking changes against released versions ([#13292](https://github.com/opensearch-project/OpenSearch/pull/13292))
- Switch to macos-13 runner for precommit and assemble github actions due to macos-latest is now arm64 ([#13412](https://github.com/opensearch-project/OpenSearch/pull/13412))
- [Revert] Prevent unnecessary fetch sub phase processor initialization during fetch phase execution ([#12503](https://github.com/opensearch-project/OpenSearch/pull/12503))

### Deprecated

### Removed
- Remove handling of index.mapper.dynamic in AutoCreateIndex([#13067](https://github.com/opensearch-project/OpenSearch/pull/13067))

### Fixed
- Fix bulk API ignores ingest pipeline for upsert ([#12883](https://github.com/opensearch-project/OpenSearch/pull/12883))
- Fix issue with feature flags where default value may not be honored ([#12849](https://github.com/opensearch-project/OpenSearch/pull/12849))
- Fix UOE While building Exists query for nested search_as_you_type field ([#12048](https://github.com/opensearch-project/OpenSearch/pull/12048))
- Client with Java 8 runtime and Apache HttpClient 5 Transport fails with java.lang.NoSuchMethodError: java.nio.ByteBuffer.flip()Ljava/nio/ByteBuffer ([#13100](https://github.com/opensearch-project/opensearch-java/pull/13100))
- Fix from and size parameter can be negative when searching ([#13047](https://github.com/opensearch-project/OpenSearch/pull/13047))
- Enabled mockTelemetryPlugin for IT and fixed OOM issues ([#13054](https://github.com/opensearch-project/OpenSearch/pull/13054))
- Fix implement mark() and markSupported() in class FilterStreamInput ([#13098](https://github.com/opensearch-project/OpenSearch/pull/13098))
- Fix IndicesRequestCache Stale calculation ([#13070](https://github.com/opensearch-project/OpenSearch/pull/13070)]
- Fix snapshot _status API to return correct status for partial snapshots ([#12812](https://github.com/opensearch-project/OpenSearch/pull/12812))
- Improve the error messages for _stats with closed indices ([#13012](https://github.com/opensearch-project/OpenSearch/pull/13012))
- Ignore BaseRestHandler unconsumed content check as it's always consumed. ([#13290](https://github.com/opensearch-project/OpenSearch/pull/13290))
- Fix mapper_parsing_exception when using flat_object fields with names longer than 11 characters ([#13259](https://github.com/opensearch-project/OpenSearch/pull/13259))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.13...2.x
