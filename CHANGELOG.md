# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added

### Dependencies
- Bump Jackson from 2.18.9 to 2.18.12 (CVE-2026-68495, CVE-2026-68496, CVE-2026-89425, CVE-2026-89407)
- Bump `com.github.luben:zstd-jni` from 1.5.5-5 to 1.5.7-25 (CVE-2026-90560, CVE-2026-87795, CVE-2026-87823, CVE-2026-90852, CVE-2026-87824, CVE-2026-87825, CVE-2026-87877, CVE-2026-89045)
- Update Jackson to 2.18.9 and Log4j to 2.25.5 ([#22990](https://github.com/opensearch-project/OpenSearch/pull/22990))
- Updated Netty to 4.1.140.Final ([#23490](https://github.com/opensearch-project/OpenSearch/pull/23490), [#23251](https://github.com/opensearch-project/OpenSearch/pull/23251), [#21491](https://github.com/opensearch-project/OpenSearch/pull/21491), [#21968](https://github.com/opensearch-project/OpenSearch/pull/21968), [#22692](https://github.com/opensearch-project/OpenSearch/pull/22692), [#23003](https://github.com/opensearch-project/OpenSearch/pull/23003))
- Update bundled JDK to 21.0.13+9 ([#23408](https://github.com/opensearch-project/OpenSearch/issues/23408), [#22572](https://github.com/opensearch-project/OpenSearch/issues/22572), [#22809](https://github.com/opensearch-project/OpenSearch/issues/22809))
- Update OpenTelemetry to 1.67.0 ([#23363](https://github.com/opensearch-project/OpenSearch/issues/23363))

### Deprecated

### Removed
- Remove identity-shiro from plugins folder ([#20305](https://github.com/opensearch-project/OpenSearch/pull/20305))

### Fixed
- Fix handling of unsupported HTTP content encodings ([#23324](https://github.com/opensearch-project/OpenSearch/pull/23324))
- Fix deadlock between engineMutex and writeLock during index close and engine reset ([#11869](https://github.com/opensearch-project/OpenSearch/issues/11869))
- Harden the circuit breaker and failure handle logic in query result consumer ([#19396](https://github.com/opensearch-project/OpenSearch/pull/19396))
- Fix case insensitive and escaped query on wildcard ([#16827](https://github.com/opensearch-project/OpenSearch/pull/16827))
- Fix array_index_out_of_bounds_exception with wildcard and aggregations ([#20842](https://github.com/opensearch-project/OpenSearch/pull/20842))
- Prevent negative fielddata stats by guarding against stale removals after shard reallocation ([#21667](https://github.com/opensearch-project/OpenSearch/pull/21667))
- Fix unbounded recursion in deserialization that can cause StackOverflowError ([#22404](https://github.com/opensearch-project/OpenSearch/pull/22404))
- Reject out-of-range WLM node threshold updates at validation time ([#22678](https://github.com/opensearch-project/OpenSearch/pull/22678))
- Add query_string nesting depth limit to prevent StackOverflow ([#22477](https://github.com/opensearch-project/OpenSearch/pull/22477))
- Run the Eclipse formatter when spotlessJavaCheck runs via precommit/check/build ([#23242](https://github.com/opensearch-project/OpenSearch/pull/23242))

### Security
- Block ptrace and process memory access syscalls in the system call filter ([#23491](https://github.com/opensearch-project/OpenSearch/pull/23491))

### Changed
