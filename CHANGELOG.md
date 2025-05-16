# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.x]
### Added
- Add support for linux riscv64 platform ([#18156](https://github.com/opensearch-project/OpenSearch/pull/18156))
- [Rule based auto-tagging] Add get rule API ([#17336](https://github.com/opensearch-project/OpenSearch/pull/17336))
- Implement parallel shard refresh behind cluster settings ([#17782](https://github.com/opensearch-project/OpenSearch/pull/17782))
- Bump OpenSearch Core main branch to 3.0.0 ([#18039](https://github.com/opensearch-project/OpenSearch/pull/18039))
- Update API of Message in index to add the timestamp for lag calculation in ingestion polling ([#17977](https://github.com/opensearch-project/OpenSearch/pull/17977/))
- Add Warm Disk Threshold Allocation Decider for Warm shards ([#18082](https://github.com/opensearch-project/OpenSearch/pull/18082))
- Add composite directory factory ([#17988](https://github.com/opensearch-project/OpenSearch/pull/17988))
- Add pull-based ingestion error metrics and make internal queue size configurable ([#18088](https://github.com/opensearch-project/OpenSearch/pull/18088))
- Adding support for derive source feature and implementing it for various type of field mappers ([#17759](https://github.com/opensearch-project/OpenSearch/pull/17759))
- [Security Manager Replacement] Enhance Java Agent to intercept newByteChannel ([#17989](https://github.com/opensearch-project/OpenSearch/pull/17989))
- Enabled Async Shard Batch Fetch by default ([#18139](https://github.com/opensearch-project/OpenSearch/pull/18139))
- Allow to get the search request from the QueryCoordinatorContext ([#17818](https://github.com/opensearch-project/OpenSearch/pull/17818))
- Improve sort-query performance by retaining the default `totalHitsThreshold` for approximated `match_all` queries ([#18189](https://github.com/opensearch-project/OpenSearch/pull/18189))
- Enable testing for ExtensiblePlugins using classpath plugins ([#16908](https://github.com/opensearch-project/OpenSearch/pull/16908))
- Introduce system generated ingest pipeline ([#17817](https://github.com/opensearch-project/OpenSearch/pull/17817)))
- Apply cluster state metadata and routing table diff when building cluster state from remote([#18256](https://github.com/opensearch-project/OpenSearch/pull/18256))
- Support create mode in pull-based ingestion and add retries for transient failures ([#18250](https://github.com/opensearch-project/OpenSearch/pull/18250)))
- Decouple the init of Crypto Plugin and KeyProvider in CryptoRegistry ([18270](https://github.com/opensearch-project/OpenSearch/pull18270)))
- Support cluster write block in pull-based ingestion ([#18280](https://github.com/opensearch-project/OpenSearch/pull/18280)))
- Use QueryCoordinatorContext for the rewrite in validate API. ([#18272](https://github.com/opensearch-project/OpenSearch/pull/18272))
- Upgrade crypto kms plugin dependencies for AWS SDK v2.x. ([#18268](https://github.com/opensearch-project/OpenSearch/pull/18268))

### Changed
- Create generic DocRequest to better categorize ActionRequests ([#18269](https://github.com/opensearch-project/OpenSearch/pull/18269)))
- Switch ingest-geoip and lang-painless modules to use the gradle version catalog ([#18316](https://github.com/opensearch-project/OpenSearch/pull/18316)))

### Dependencies
- Bump `com.google.code.gson:gson` from 2.12.1 to 2.13.1 ([#17923](https://github.com/opensearch-project/OpenSearch/pull/17923), [#18266](https://github.com/opensearch-project/OpenSearch/pull/18266))
- Bump `com.github.spotbugs:spotbugs-annotations` from 4.9.0 to 4.9.3 ([#17922](https://github.com/opensearch-project/OpenSearch/pull/17922))
- Bump `com.microsoft.azure:msal4j` from 1.18.0 to 1.20.0 ([#17925](https://github.com/opensearch-project/OpenSearch/pull/17925))
- Update Apache HttpClient5 and HttpCore5 (CVE-2025-27820) ([#18152](https://github.com/opensearch-project/OpenSearch/pull/18152))
- Bump `org.apache.commons:commons-collections4` from 4.4 to 4.5.0 ([#18101](https://github.com/opensearch-project/OpenSearch/pull/18101))
- Bump `netty` from 4.1.118.Final to 4.1.121.Final ([#18192](https://github.com/opensearch-project/OpenSearch/pull/18192))
- Bump `org.apache.commons:commons-configuration2` from 2.11.0 to 2.12.0 ([#18103](https://github.com/opensearch-project/OpenSearch/pull/18103), [#18262](https://github.com/opensearch-project/OpenSearch/pull/18262))
- Bump `com.nimbusds:nimbus-jose-jwt` from 10.0.2 to 10.2 ([#18104](https://github.com/opensearch-project/OpenSearch/pull/18104))
- Bump `org.apache.commons:commons-text` from 1.13.0 to 1.13.1 ([#18102](https://github.com/opensearch-project/OpenSearch/pull/18102))
- Bump `reactor-netty` from 1.2.4 to 1.2.5 ([#18243](https://github.com/opensearch-project/OpenSearch/pull/18243))
- Bump `reactor` from 3.5.20 to 3.7.5 ([#18243](https://github.com/opensearch-project/OpenSearch/pull/18243))
- Bump `com.azure:azure-core-http-netty` from 1.15.7 to 1.15.11 ([#18265](https://github.com/opensearch-project/OpenSearch/pull/18265))
- Bump `lycheeverse/lychee-action` from 2.4.0 to 2.4.1 ([#18264](https://github.com/opensearch-project/OpenSearch/pull/18264))
- Bump `com.maxmind.geoip2:geoip2` from 4.2.1 to 4.3.0 ([#18263](https://github.com/opensearch-project/OpenSearch/pull/18263))

### Deprecated

### Removed

### Fixed
- Fix simultaneously creating a snapshot and updating the repository can potentially trigger an infinite loop ([#17532](https://github.com/opensearch-project/OpenSearch/pull/17532))
- Remove package org.opensearch.transport.grpc and replace with org.opensearch.plugin.transport.grpc ([#18031](https://github.com/opensearch-project/OpenSearch/pull/18031))
- Fix the native plugin installation error cause by the pgp public key change ([#18147](https://github.com/opensearch-project/OpenSearch/pull/18147))
- Fix object field exists query ([#17843](https://github.com/opensearch-project/OpenSearch/pull/17843))
- Use Bad Request status for InputCoercionException ([#18161](https://github.com/opensearch-project/OpenSearch/pull/18161))
- Null check field names in QueryStringQueryBuilder ([#18194](https://github.com/opensearch-project/OpenSearch/pull/18194))
- Avoid NPE if on SnapshotInfo if 'shallow' boolean not present ([#18187](https://github.com/opensearch-project/OpenSearch/issues/18187))
- Fix 'system call filter not installed' caused when network.host: 0.0.0.0 ([#18309](https://github.com/opensearch-project/OpenSearch/pull/18309))

### Security

[Unreleased 3.x]: https://github.com/opensearch-project/OpenSearch/compare/3.0...main
