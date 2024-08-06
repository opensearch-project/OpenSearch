# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Tiered caching] Introducing cache plugins and exposing Ehcache as one of the pluggable disk cache option ([#11874](https://github.com/opensearch-project/OpenSearch/pull/11874))
- Add support for dependencies in plugin descriptor properties with semver range ([#11441](https://github.com/opensearch-project/OpenSearch/pull/11441))
- Add community_id ingest processor ([#12121](https://github.com/opensearch-project/OpenSearch/pull/12121))
- Introduce query level setting `index.query.max_nested_depth` limiting nested queries ([#3268](https://github.com/opensearch-project/OpenSearch/issues/3268)
- Add toString methods to MultiSearchRequest, MultiGetRequest and CreateIndexRequest ([#12163](https://github.com/opensearch-project/OpenSearch/pull/12163))
- Fix error in RemoteSegmentStoreDirectory when debug logging is enabled ([#12328](https://github.com/opensearch-project/OpenSearch/pull/12328))
- Support for returning scores in matched queries ([#11626](https://github.com/opensearch-project/OpenSearch/pull/11626))
- Add shard id property to SearchLookup for use in field types provided by plugins ([#1063](https://github.com/opensearch-project/OpenSearch/pull/1063))
- Add kuromoji_completion analyzer and filter ([#4835](https://github.com/opensearch-project/OpenSearch/issues/4835))
- [Admission Control] Integrate IO Usage Tracker to the Resource Usage Collector Service and Emit IO Usage Stats ([#11880](https://github.com/opensearch-project/OpenSearch/pull/11880))
- The org.opensearch.bootstrap.Security should support codebase for JAR files with classifiers ([#12586](https://github.com/opensearch-project/OpenSearch/issues/12586))
- Add took time to request nodes stats ([#15054](https://github.com/opensearch-project/OpenSearch/pull/15054))

### Dependencies
- Bump `com.squareup.okio:okio` from 3.7.0 to 3.8.0 ([#12290](https://github.com/opensearch-project/OpenSearch/pull/12290))
- Bump `org.bouncycastle:bcprov-jdk15to18` to `org.bouncycastle:bcprov-jdk18on` version 1.77 ([#12326](https://github.com/opensearch-project/OpenSearch/pull/12326))
- Bump `org.bouncycastle:bcmail-jdk15to18` to `org.bouncycastle:bcmail-jdk18on` version 1.77 ([#12326](https://github.com/opensearch-project/OpenSearch/pull/12326))
- Bump `org.bouncycastle:bcpkix-jdk15to18` to `org.bouncycastle:bcpkix-jdk18on` version 1.77 ([#12326](https://github.com/opensearch-project/OpenSearch/pull/12326))
- Bump `gradle/wrapper-validation-action` from 1 to 2 ([#12367](https://github.com/opensearch-project/OpenSearch/pull/12367))
- Bump `netty` from 4.1.106.Final to 4.1.107.Final ([#12372](https://github.com/opensearch-project/OpenSearch/pull/12372))
- Bump `opentelemetry` from 1.34.1 to 1.36.0 ([#12388](https://github.com/opensearch-project/OpenSearch/pull/12388), [#12618](https://github.com/opensearch-project/OpenSearch/pull/12618))
- Bump Apache Lucene from 9.9.2 to 9.10.0 ([#12392](https://github.com/opensearch-project/OpenSearch/pull/12392))
- Bump `org.apache.logging.log4j:log4j-core` from 2.22.1 to 2.23.1 ([#12464](https://github.com/opensearch-project/OpenSearch/pull/12464), [#12587](https://github.com/opensearch-project/OpenSearch/pull/12587))
- Bump `antlr4` from 4.11.1 to 4.13.1 ([#12445](https://github.com/opensearch-project/OpenSearch/pull/12445))
- Bump `com.netflix.nebula.ospackage-base` from 11.8.0 to 11.8.1 ([#12461](https://github.com/opensearch-project/OpenSearch/pull/12461))
- Bump `peter-evans/create-or-update-comment` from 3 to 4 ([#12462](https://github.com/opensearch-project/OpenSearch/pull/12462))
- Bump `lycheeverse/lychee-action` from 1.9.1 to 1.9.3 ([#12521](https://github.com/opensearch-project/OpenSearch/pull/12521))
- Bump `com.azure:azure-core` from 1.39.0 to 1.47.0 ([#12520](https://github.com/opensearch-project/OpenSearch/pull/12520))
- Bump `ch.qos.logback:logback-core` from 1.2.13 to 1.5.3 ([#12519](https://github.com/opensearch-project/OpenSearch/pull/12519))
- Bump `codecov/codecov-action` from 3 to 4 ([#12585](https://github.com/opensearch-project/OpenSearch/pull/12585))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.1 to 3.9.2 ([#12580](https://github.com/opensearch-project/OpenSearch/pull/12580))
- Bump `org.codehaus.woodstox:stax2-api` from 4.2.1 to 4.2.2 ([#12579](https://github.com/opensearch-project/OpenSearch/pull/12579))
- Bump Jackson version from 2.16.1 to 2.16.2 ([#12611](https://github.com/opensearch-project/OpenSearch/pull/12611))

### Changed
- Allow composite aggregation to run under a parent filter aggregation ([#11499](https://github.com/opensearch-project/OpenSearch/pull/11499))

### Deprecated

### Removed

### Fixed
- [Revert] [Bug] Check phase name before SearchRequestOperationsListener onPhaseStart ([#12035](https://github.com/opensearch-project/OpenSearch/pull/12035))
- Add support of special WrappingSearchAsyncActionPhase so the onPhaseStart() will always be followed by onPhaseEnd() within AbstractSearchAsyncAction ([#12293](https://github.com/opensearch-project/OpenSearch/pull/12293))
- Add a system property to configure YamlParser codepoint limits ([#12298](https://github.com/opensearch-project/OpenSearch/pull/12298))
- Prevent read beyond slice boundary in ByteArrayIndexInput ([#10481](https://github.com/opensearch-project/OpenSearch/issues/10481))
- Fix the "highlight.max_analyzer_offset" request parameter with "plain" highlighter ([#10919](https://github.com/opensearch-project/OpenSearch/pull/10919))
- Fix `terms` query on `float` field when `doc_values` are turned off by reverting back to `FloatPoint` from `FloatField` ([#12499](https://github.com/opensearch-project/OpenSearch/pull/12499))
- Fix get task API does not refresh resource stats ([#11531](https://github.com/opensearch-project/OpenSearch/pull/11531))
- Fix for deserilization bug in weighted round-robin metadata ([#11679](https://github.com/opensearch-project/OpenSearch/pull/11679))
- onShardResult and onShardFailure are executed on one shard causes opensearch jvm crashed ([#12158](https://github.com/opensearch-project/OpenSearch/pull/12158))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.12...2.x
