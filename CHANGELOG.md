# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add fingerprint ingest processor ([#13724](https://github.com/opensearch-project/OpenSearch/pull/13724))
- [Remote Store] Rate limiter for remote store low priority uploads ([#14374](https://github.com/opensearch-project/OpenSearch/pull/14374/))
- Apply the date histogram rewrite optimization to range aggregation ([#13865](https://github.com/opensearch-project/OpenSearch/pull/13865))
- [Writable Warm] Add composite directory implementation and integrate it with FileCache ([12782](https://github.com/opensearch-project/OpenSearch/pull/12782))
- Fix race condition while parsing derived fields from search definition ([14445](https://github.com/opensearch-project/OpenSearch/pull/14445))

### Dependencies
- Update to Apache Lucene 9.11.0 ([#14042](https://github.com/opensearch-project/OpenSearch/pull/14042))
- Bump `netty` from 4.1.110.Final to 4.1.111.Final ([#14356](https://github.com/opensearch-project/OpenSearch/pull/14356))
- Bump `org.wiremock:wiremock-standalone` from 3.3.1 to 3.6.0 ([#14361](https://github.com/opensearch-project/OpenSearch/pull/14361))
- Bump `reactor` from 3.5.17 to 3.5.18 ([#14395](https://github.com/opensearch-project/OpenSearch/pull/14395))
- Bump `reactor-netty` from 1.1.19 to 1.1.20 ([#14395](https://github.com/opensearch-project/OpenSearch/pull/14395))
- Bump `commons-net:commons-net` from 3.10.0 to 3.11.1 ([#14396](https://github.com/opensearch-project/OpenSearch/pull/14396))
- Bump `com.nimbusds:nimbus-jose-jwt` from 9.37.3 to 9.40 ([#14398](https://github.com/opensearch-project/OpenSearch/pull/14398))
- Bump `org.apache.commons:commons-configuration2` from 2.10.1 to 2.11.0 ([#14399](https://github.com/opensearch-project/OpenSearch/pull/14399))
- Bump `com.gradle.develocity` from 3.17.4 to 3.17.5 ([#14397](https://github.com/opensearch-project/OpenSearch/pull/14397))
- Bump `opentelemetry` from 1.36.0 to 1.39.0 ([#14457](https://github.com/opensearch-project/OpenSearch/pull/14457))
- Bump `azure-identity` from 1.11.4 to 1.13.0, Bump `msal4j` from 1.14.3 to 1.15.1, Bump `msal4j-persistence-extension` from 1.2.0 to 1.3.0 ([#14506](https://github.com/opensearch-project/OpenSearch/pull/14506))

### Changed
- unsignedLongRangeQuery now returns MatchNoDocsQuery if the lower bounds are greater than the upper bounds ([#14416](https://github.com/opensearch-project/OpenSearch/pull/14416))
- Make the class CommunityIdProcessor final ([#14448](https://github.com/opensearch-project/OpenSearch/pull/14448))
- Updated the `indices.query.bool.max_clause_count` setting from being static to dynamically updateable ([#13568](https://github.com/opensearch-project/OpenSearch/pull/13568))

### Deprecated

### Removed

### Fixed
- Fix bug in SBP cancellation logic ([#13259](https://github.com/opensearch-project/OpenSearch/pull/13474))
- Fix handling of Short and Byte data types in ScriptProcessor ingest pipeline ([#14379](https://github.com/opensearch-project/OpenSearch/issues/14379))
- Switch to iterative version of WKT format parser ([#14086](https://github.com/opensearch-project/OpenSearch/pull/14086))
- Fix the computed max shards of cluster to avoid int overflow ([#14155](https://github.com/opensearch-project/OpenSearch/pull/14155))
- Fixed rest-high-level client searchTemplate & mtermVectors endpoints to have a leading slash ([#14465](https://github.com/opensearch-project/OpenSearch/pull/14465))
- Write shard level metadata blob when snapshotting searchable snapshot indexes ([#13190](https://github.com/opensearch-project/OpenSearch/pull/13190))
- Fix aggs result of NestedAggregator with sub NestedAggregator ([#13324](https://github.com/opensearch-project/OpenSearch/pull/13324))
- Fix fs info reporting negative available size ([#11573](https://github.com/opensearch-project/OpenSearch/pull/11573))
- Add ListPitInfo::getKeepAlive() getter ([#14495](https://github.com/opensearch-project/OpenSearch/pull/14495))
- Fix FuzzyQuery in keyword field will use IndexOrDocValuesQuery when both of index and doc_value are true ([#14378](https://github.com/opensearch-project/OpenSearch/pull/14378))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.15...2.x
