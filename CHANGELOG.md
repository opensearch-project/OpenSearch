# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for Warm Indices Write Block on Flood Watermark breach ([#18375](https://github.com/opensearch-project/OpenSearch/pull/18375))
- Add support for linux riscv64 platform ([#18156](https://github.com/opensearch-project/OpenSearch/pull/18156))
- [Rule based auto-tagging] Add get rule API ([#17336](https://github.com/opensearch-project/OpenSearch/pull/17336))
- [Rule based auto-tagging] Add Delete Rule API ([#18184](https://github.com/opensearch-project/OpenSearch/pull/18184))
- Add paginated wlm/stats API ([#17638](https://github.com/opensearch-project/OpenSearch/pull/17638))
- [Rule based auto-tagging] Add Create rule API ([#17792](https://github.com/opensearch-project/OpenSearch/pull/17792))
- Implement parallel shard refresh behind cluster settings ([#17782](https://github.com/opensearch-project/OpenSearch/pull/17782))
- Bump OpenSearch Core main branch to 3.0.0 ([#18039](https://github.com/opensearch-project/OpenSearch/pull/18039))
- [Rule based Auto-tagging] Add wlm `ActionFilter` ([#17791](https://github.com/opensearch-project/OpenSearch/pull/17791))
- [Rule based auto-tagging] Add update rule API ([#17797](https://github.com/opensearch-project/OpenSearch/pull/17797))
- [Rule based auto-tagging] Bug fix for update rule api ([#18488](https://github.com/opensearch-project/OpenSearch/pull/18488))
- [Rule based auto-tagging] Add Rule based auto-tagging IT ([#18550](https://github.com/opensearch-project/OpenSearch/pull/18550))
- Update API of Message in index to add the timestamp for lag calculation in ingestion polling ([#17977](https://github.com/opensearch-project/OpenSearch/pull/17977/))
- Add Warm Disk Threshold Allocation Decider for Warm shards ([#18082](https://github.com/opensearch-project/OpenSearch/pull/18082))
- Add composite directory factory ([#17988](https://github.com/opensearch-project/OpenSearch/pull/17988))
- [Rule based auto-tagging] Add refresh based synchronization service for `Rule`s ([#18128](https://github.com/opensearch-project/OpenSearch/pull/18128))
- Add pull-based ingestion error metrics and make internal queue size configurable ([#18088](https://github.com/opensearch-project/OpenSearch/pull/18088))
- Adding support for derive source feature and implementing it for various type of field mappers ([#17759](https://github.com/opensearch-project/OpenSearch/pull/17759))
- [Security Manager Replacement] Enhance Java Agent to intercept newByteChannel ([#17989](https://github.com/opensearch-project/OpenSearch/pull/17989))
- Enabled Async Shard Batch Fetch by default ([#18139](https://github.com/opensearch-project/OpenSearch/pull/18139))
- Allow to get the search request from the QueryCoordinatorContext ([#17818](https://github.com/opensearch-project/OpenSearch/pull/17818))
- Reject close index requests, while remote store migration is in progress.([#18327](https://github.com/opensearch-project/OpenSearch/pull/18327))
- Improve sort-query performance by retaining the default `totalHitsThreshold` for approximated `match_all` queries ([#18189](https://github.com/opensearch-project/OpenSearch/pull/18189))
- Enable testing for ExtensiblePlugins using classpath plugins ([#16908](https://github.com/opensearch-project/OpenSearch/pull/16908))
- Introduce system generated ingest pipeline ([#17817](https://github.com/opensearch-project/OpenSearch/pull/17817)))
- Added Auto Force Merge Manager to enhance hot to warm migration experience ([#18229](https://github.com/opensearch-project/OpenSearch/pull/18229))
- Apply cluster state metadata and routing table diff when building cluster state from remote([#18256](https://github.com/opensearch-project/OpenSearch/pull/18256))
- Support create mode in pull-based ingestion and add retries for transient failures ([#18250](https://github.com/opensearch-project/OpenSearch/pull/18250)))
- Decouple the init of Crypto Plugin and KeyProvider in CryptoRegistry ([18270](https://github.com/opensearch-project/OpenSearch/pull18270)))
- Support cluster write block in pull-based ingestion ([#18280](https://github.com/opensearch-project/OpenSearch/pull/18280)))
- Use QueryCoordinatorContext for the rewrite in validate API. ([#18272](https://github.com/opensearch-project/OpenSearch/pull/18272))
- Upgrade crypto kms plugin dependencies for AWS SDK v2.x. ([#18268](https://github.com/opensearch-project/OpenSearch/pull/18268))
- Add support for `matched_fields` with the unified highlighter ([#18164](https://github.com/opensearch-project/OpenSearch/issues/18164))
- Add BooleanQuery rewrite for must_not RangeQuery clauses ([#17655](https://github.com/opensearch-project/OpenSearch/pull/17655))
- [repository-s3] Add support for SSE-KMS and S3 bucket owner verification ([#18312](https://github.com/opensearch-project/OpenSearch/pull/18312))
- Optimize gRPC perf by passing by reference ([#18303](https://github.com/opensearch-project/OpenSearch/pull/18303))
- Added File Cache Stats - Involves Block level as well as full file level stats ([#17538](https://github.com/opensearch-project/OpenSearch/issues/17479))
- Added time_in_execution attribute to /_cluster/pending_tasks response ([#17780](https://github.com/opensearch-project/OpenSearch/pull/17780))
- Added File Cache Pinning ([#17617](https://github.com/opensearch-project/OpenSearch/issues/13648))
- Support consumer reset in Resume API for pull-based ingestion. This PR includes a breaking change for the experimental pull-based ingestion feature. ([#18332](https://github.com/opensearch-project/OpenSearch/pull/18332))
- Add FIPS build tooling ([#4254](https://github.com/opensearch-project/security/issues/4254))
- Support Nested Aggregations as part of Star-Tree ([#18048](https://github.com/opensearch-project/OpenSearch/pull/18048))
- [Star-Tree] Support for date-range queries with star-tree supported aggregations ([#17855](https://github.com/opensearch-project/OpenSearch/pull/17855)
- Added node-left metric to cluster manager ([#18421](https://github.com/opensearch-project/OpenSearch/pull/18421))
- [Star tree] Remove star tree feature flag and add index setting to configure star tree search on index basis ([#18070](https://github.com/opensearch-project/OpenSearch/pull/18070))
- Approximation Framework Enhancement: Update the BKD traversal logic to improve the performance on skewed data ([#18439](https://github.com/opensearch-project/OpenSearch/issues/18439))
- Support system generated ingest pipelines for bulk update operations ([#18277](https://github.com/opensearch-project/OpenSearch/pull/18277)))
- Added FS Health Check Failure metric ([#18435](https://github.com/opensearch-project/OpenSearch/pull/18435))
- Ability to run Code Coverage with Gradle and produce the jacoco reports locally ([#18509](https://github.com/opensearch-project/OpenSearch/issues/18509))
- Add NodeResourceUsageStats to ClusterInfo ([#18480](https://github.com/opensearch-project/OpenSearch/issues/18472))
- Introduce SecureHttpTransportParameters experimental API (to complement SecureTransportParameters counterpart) ([#18572](https://github.com/opensearch-project/OpenSearch/issues/18572))
- Create equivalents of JSM's AccessController in the java agent ([#18346](https://github.com/opensearch-project/OpenSearch/issues/18346))
- Introduced a new cluster-level API to fetch remote store metadata (segments and translogs) for each shard of an index. ([#18257](https://github.com/opensearch-project/OpenSearch/pull/18257))
- Add last index request timestamp columns to the `_cat/indices` API. ([10766](https://github.com/opensearch-project/OpenSearch/issues/10766))
- Introduce a new pull-based ingestion plugin for file-based indexing (for local testing) ([#18591](https://github.com/opensearch-project/OpenSearch/pull/18591))
- Add support for search pipeline in search and msearch template ([#18564](https://github.com/opensearch-project/OpenSearch/pull/18564))

### Changed
- Update Subject interface to use CheckedRunnable ([#18570](https://github.com/opensearch-project/OpenSearch/issues/18570))

### Dependencies
- Bump `stefanzweifel/git-auto-commit-action` from 5 to 6 ([#18524](https://github.com/opensearch-project/OpenSearch/pull/18524))
- Bump Apache Lucene to 10.2.2 ([#18573](https://github.com/opensearch-project/OpenSearch/pull/18573))
- Bump `org.apache.logging.log4j:log4j-core` from 2.24.3 to 2.25.0 ([#18589](https://github.com/opensearch-project/OpenSearch/pull/18589))
- Bump `com.google.code.gson:gson` from 2.13.0 to 2.13.1 ([#18585](https://github.com/opensearch-project/OpenSearch/pull/18585))
- Bump `com.azure:azure-core-http-netty` from 1.15.11 to 1.15.12 ([#18586](https://github.com/opensearch-project/OpenSearch/pull/18586))

### Deprecated

### Removed

### Fixed
- Add task cancellation checks in aggregators ([#18426](https://github.com/opensearch-project/OpenSearch/pull/18426))
- Fix concurrent timings in profiler ([#18540](https://github.com/opensearch-project/OpenSearch/pull/18540))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.1...main
