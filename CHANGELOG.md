# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add support for async deletion in S3BlobContainer ([#15621](https://github.com/opensearch-project/OpenSearch/pull/15621))
- MultiTermQueries in keyword fields now default to `indexed` approach and gated behind cluster setting ([#15637](https://github.com/opensearch-project/OpenSearch/pull/15637))
- [Workload Management] QueryGroup resource cancellation framework changes ([#15651](https://github.com/opensearch-project/OpenSearch/pull/15651))
- Fallback to Remote cluster-state on Term-Version check mismatch - ([#15424](https://github.com/opensearch-project/OpenSearch/pull/15424))
- Implement WithFieldName interface in ValuesSourceAggregationBuilder & FieldSortBuilder ([#15916](https://github.com/opensearch-project/OpenSearch/pull/15916))
- Add successfulSearchShardIndices in searchRequestContext ([#15967](https://github.com/opensearch-project/OpenSearch/pull/15967))
- Remove identity-related feature flagged code from the RestController ([#15430](https://github.com/opensearch-project/OpenSearch/pull/15430))
- Add support for msearch API to pass search pipeline name - ([#15923](https://github.com/opensearch-project/OpenSearch/pull/15923))
- Add _list/indices API as paginated alternate to _cat/indices ([#14718](https://github.com/opensearch-project/OpenSearch/pull/14718))
- Add success and failure metrics for async shard fetch ([#15976](https://github.com/opensearch-project/OpenSearch/pull/15976))

### Dependencies
- Bump `com.azure:azure-identity` from 1.13.0 to 1.13.2 ([#15578](https://github.com/opensearch-project/OpenSearch/pull/15578))
- Bump `protobuf` from 3.22.3 to 3.25.4 ([#15684](https://github.com/opensearch-project/OpenSearch/pull/15684))
- Bump `org.apache.logging.log4j:log4j-core` from 2.23.1 to 2.24.1 ([#15858](https://github.com/opensearch-project/OpenSearch/pull/15858), [#16134](https://github.com/opensearch-project/OpenSearch/pull/16134))
- Bump `peter-evans/create-pull-request` from 6 to 7 ([#15863](https://github.com/opensearch-project/OpenSearch/pull/15863))
- Bump `com.nimbusds:oauth2-oidc-sdk` from 11.9.1 to 11.19.1 ([#15862](https://github.com/opensearch-project/OpenSearch/pull/15862))
- Bump `com.microsoft.azure:msal4j` from 1.17.0 to 1.17.1 ([#15945](https://github.com/opensearch-project/OpenSearch/pull/15945))
- Bump `ch.qos.logback:logback-core` from 1.5.6 to 1.5.8 ([#15946](https://github.com/opensearch-project/OpenSearch/pull/15946))
- Update protobuf from 3.25.4 to 3.25.5 ([#16011](https://github.com/opensearch-project/OpenSearch/pull/16011))
- Bump `org.roaringbitmap:RoaringBitmap` from 1.2.1 to 1.3.0 ([#16040](https://github.com/opensearch-project/OpenSearch/pull/16040))
- Bump `com.nimbusds:nimbus-jose-jwt` from 9.40 to 9.41.1 ([#16038](https://github.com/opensearch-project/OpenSearch/pull/16038))
- Bump `actions/github-script` from 5 to 7 ([#16039](https://github.com/opensearch-project/OpenSearch/pull/16039))
- Bump `dnsjava:dnsjava` from 3.6.1 to 3.6.2 ([#16041](https://github.com/opensearch-project/OpenSearch/pull/16041))
- Bump `com.maxmind.geoip2:geoip2` from 4.2.0 to 4.2.1 ([#16042](https://github.com/opensearch-project/OpenSearch/pull/16042))

### Changed
- Add support for docker compose v2 in TestFixturesPlugin ([#16049](https://github.com/opensearch-project/OpenSearch/pull/16049))


### Deprecated

### Removed

### Fixed
- Fix wildcard query containing escaped character ([#15737](https://github.com/opensearch-project/OpenSearch/pull/15737))
- Fix case-insensitive query on wildcard field ([#15882](https://github.com/opensearch-project/OpenSearch/pull/15882))
- Add validation for the search backpressure cancellation settings ([#15501](https://github.com/opensearch-project/OpenSearch/pull/15501))
- Fix search_as_you_type not supporting multi-fields ([#15988](https://github.com/opensearch-project/OpenSearch/pull/15988))
- Avoid infinite loop when `flat_object` field contains invalid token ([#15985](https://github.com/opensearch-project/OpenSearch/pull/15985))
- Fix infinite loop in nested agg ([#15931](https://github.com/opensearch-project/OpenSearch/pull/15931))
- Fix race condition in node-join and node-left ([#15521](https://github.com/opensearch-project/OpenSearch/pull/15521))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.17...2.x
