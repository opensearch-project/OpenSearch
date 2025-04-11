# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Change priority for scheduling reroute during timeout([#16445](https://github.com/opensearch-project/OpenSearch/pull/16445))
- [Rule Based Auto-tagging] Add rule schema for auto tagging ([#17238](https://github.com/opensearch-project/OpenSearch/pull/17238))
- Renaming the node role search to warm ([#17573](https://github.com/opensearch-project/OpenSearch/pull/17573))
- Introduce a new search node role to hold search only shards ([#17620](https://github.com/opensearch-project/OpenSearch/pull/17620))
- Fix systemd integTest on deb regarding path ownership check ([#17641](https://github.com/opensearch-project/OpenSearch/pull/17641))
- Add dfs transformation function in XContentMapValues ([#17612](https://github.com/opensearch-project/OpenSearch/pull/17612))
- Add tracking for long-running SearchTask post cancellation ([#17726](https://github.com/opensearch-project/OpenSearch/pull/17726))
- Added Kinesis support as a plugin for the pull-based ingestion ([#17615](https://github.com/opensearch-project/OpenSearch/pull/17615))
- Add FilterFieldType for developers who want to wrap MappedFieldType ([#17627](https://github.com/opensearch-project/OpenSearch/pull/17627))
- [Rule Based Auto-tagging] Add in-memory rule processing service ([#17365](https://github.com/opensearch-project/OpenSearch/pull/17365))
- [Security Manager Replacement] Create initial Java Agent to intercept Socket::connect calls ([#17724](https://github.com/opensearch-project/OpenSearch/pull/17724))
- Faster `terms_query` with already sorted terms ([#17714](https://github.com/opensearch-project/OpenSearch/pull/17714))
- Add ingestion management APIs for pause, resume and get ingestion state ([#17631](https://github.com/opensearch-project/OpenSearch/pull/17631))
- [Security Manager Replacement] Enhance Java Agent to intercept System::exit ([#17746](https://github.com/opensearch-project/OpenSearch/pull/17746))
- [Security Manager Replacement] Add a policy parser for Java agent security policies ([#17753](https://github.com/opensearch-project/OpenSearch/pull/17753))
- [Security Manager Replacement] Implement File Interceptor and add integration tests ([#17760](https://github.com/opensearch-project/OpenSearch/pull/17760))
- [Security Manager Replacement] Enhance Java Agent to intercept Runtime::halt ([#17757](https://github.com/opensearch-project/OpenSearch/pull/17757))
- [Security Manager Replacement] Phase off SecurityManager usage in favor of Java Agent ([#17861](https://github.com/opensearch-project/OpenSearch/pull/17861))
- Support AutoExpand for SearchReplica ([#17741](https://github.com/opensearch-project/OpenSearch/pull/17741))
- Add TLS enabled SecureNetty4GrpcServerTransport ([#17796](https://github.com/opensearch-project/OpenSearch/pull/17796))
- Implement fixed interval refresh task scheduling ([#17777](https://github.com/opensearch-project/OpenSearch/pull/17777))
- [Tiered caching] Create a single cache manager for all the disk caches. ([#17513](https://github.com/opensearch-project/OpenSearch/pull/17513))
- Add GRPC DocumentService and Bulk endpoint ([#17727](https://github.com/opensearch-project/OpenSearch/pull/17727))
- Added scale to zero (`search_only` mode) support for OpenSearch reader writer separation ([#17299](https://github.com/opensearch-project/OpenSearch/pull/17299)
- [Star Tree] [Search] Resolving numeric range aggregation with metric aggregation using star-tree ([#17273](https://github.com/opensearch-project/OpenSearch/pull/17273))
- Added Search Only strict routing setting ([#17803](https://github.com/opensearch-project/OpenSearch/pull/17803))
- Disable the index API for ingestion engine ([#17768](https://github.com/opensearch-project/OpenSearch/pull/17768))
- Add SearchService and Search GRPC endpoint ([#17830](https://github.com/opensearch-project/OpenSearch/pull/17830))
- Add update and delete support in pull-based ingestion ([#17822](https://github.com/opensearch-project/OpenSearch/pull/17822))
- Allow maxPollSize and pollTimeout in IngestionSource to be configurable ([#17863](https://github.com/opensearch-project/OpenSearch/pull/17863))

### Changed
- Migrate BC libs to their FIPS counterparts ([#14912](https://github.com/opensearch-project/OpenSearch/pull/14912))
- Increase the floor segment size to 16MB ([#17699](https://github.com/opensearch-project/OpenSearch/pull/17699))
- Unwrap singleton DocValues in global ordinal value source of composite histogram aggregation ([#17740](https://github.com/opensearch-project/OpenSearch/pull/17740))
- Unwrap singleton DocValues in date histogram aggregation. ([#17643](https://github.com/opensearch-project/OpenSearch/pull/17643))
- Introduce 512 byte limit to search and ingest pipeline IDs ([#17786](https://github.com/opensearch-project/OpenSearch/pull/17786))
- Avoid skewed segment replication lag metric ([#17831](https://github.com/opensearch-project/OpenSearch/pull/17831))
- Increase the default segment counter step size when replica promoting ([#17568](https://github.com/opensearch-project/OpenSearch/pull/17568))

### Dependencies
- Bump `com.nimbusds:nimbus-jose-jwt` from 9.41.1 to 10.0.2 ([#17607](https://github.com/opensearch-project/OpenSearch/pull/17607), [#17669](https://github.com/opensearch-project/OpenSearch/pull/17669))
- Bump `com.google.api:api-common` from 1.8.1 to 2.46.1 ([#17604](https://github.com/opensearch-project/OpenSearch/pull/17604))
- Bump `ch.qos.logback:logback-core` from 1.5.16 to 1.5.18 ([#17609](https://github.com/opensearch-project/OpenSearch/pull/17609), [#17809](https://github.com/opensearch-project/OpenSearch/pull/17809))
- Bump `org.jruby.joni:joni` from 2.2.3 to 2.2.6 ([#17608](https://github.com/opensearch-project/OpenSearch/pull/17608), [#17732](https://github.com/opensearch-project/OpenSearch/pull/17732))
- Bump `dangoslen/dependabot-changelog-helper` from 3 to 4 ([#17498](https://github.com/opensearch-project/OpenSearch/pull/17498))
- Bump `com.google.api:gax` from 2.35.0 to 2.63.1 ([#17465](https://github.com/opensearch-project/OpenSearch/pull/17465))
- Bump `com.azure:azure-storage-blob` from 12.29.1 to 12.30.0 ([#17667](https://github.com/opensearch-project/OpenSearch/pull/17667))
- Bump `tj-actions/changed-files` from 46.0.1 to 46.0.4 ([#17666](https://github.com/opensearch-project/OpenSearch/pull/17666), [#17813](https://github.com/opensearch-project/OpenSearch/pull/17813))
- Bump `com.google.code.gson:gson` from 2.11.0 to 2.12.1 ([#17668](https://github.com/opensearch-project/OpenSearch/pull/17668))
- Bump `com.github.luben:zstd-jni` from 1.5.5-1 to 1.5.6-1 ([#17674](https://github.com/opensearch-project/OpenSearch/pull/17674))
- Bump `lycheeverse/lychee-action` from 2.3.0 to 2.4.0 ([#17731](https://github.com/opensearch-project/OpenSearch/pull/17731))
- Bump `com.netflix.nebula.ospackage-base` from 11.11.1 to 11.11.2 ([#17734](https://github.com/opensearch-project/OpenSearch/pull/17734))
- Bump `com.nimbusds:oauth2-oidc-sdk` from 11.21 to 11.23.1 ([#17729](https://github.com/opensearch-project/OpenSearch/pull/17729))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.52.0 to 2.54.1 ([#17733](https://github.com/opensearch-project/OpenSearch/pull/17733))
- Bump `ch.qos.logback:logback-classic` from 1.5.17 to 1.5.18 ([#17730](https://github.com/opensearch-project/OpenSearch/pull/17730))
- Bump `reactor_netty` from 1.1.26 to 1.2.3 ([#17322](https://github.com/opensearch-project/OpenSearch/pull/17322), [#17377](https://github.com/opensearch-project/OpenSearch/pull/17377))
- Bump `com.google.api.grpc:proto-google-iam-v1` from 1.33.0 to 1.49.1 ([#17811](https://github.com/opensearch-project/OpenSearch/pull/17811))
- Bump `com.azure:azure-core` from 1.54.1 to 1.55.3 ([#17810](https://github.com/opensearch-project/OpenSearch/pull/17810))
- Bump `org.apache.poi` version from 5.2.5 to 5.4.1 in /plugins/ingest-attachment ([#17887](https://github.com/opensearch-project/OpenSearch/pull/17887))

### Changed

### Deprecated

### Removed
- Remove deprecated `batch_size` parameter from `_bulk` ([#14283](https://github.com/opensearch-project/OpenSearch/issues/14283))
- Remove `FeatureFlags.APPROXIMATE_POINT_RANGE_QUERY_SETTING` since range query approximation is no longer experimental ([#17769](https://github.com/opensearch-project/OpenSearch/pull/17769))

### Fixed
- Fix bytes parameter on `_cat/recovery` ([#17598](https://github.com/opensearch-project/OpenSearch/pull/17598))
- Fix slow performance of FeatureFlag checks ([#17611](https://github.com/opensearch-project/OpenSearch/pull/17611))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/f58d846f...main
