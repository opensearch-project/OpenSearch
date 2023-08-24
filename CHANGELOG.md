# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add server version as REST response header [#6583](https://github.com/opensearch-project/OpenSearch/issues/6583)
- Start replication checkpointTimers on primary before segments upload to remote store. ([#8221]()https://github.com/opensearch-project/OpenSearch/pull/8221)
- Introduce new static cluster setting to control slice computation for concurrent segment search. ([#8847](https://github.com/opensearch-project/OpenSearch/pull/8884))
- Add configuration for file cache size to max remote data ratio to prevent oversubscription of file cache ([#8606](https://github.com/opensearch-project/OpenSearch/pull/8606))
- Disallow compression level to be set for default and best_compression index codecs ([#8737]()https://github.com/opensearch-project/OpenSearch/pull/8737)
- [distribution/archives] [Linux] [x64] Provide the variant of the distributions bundled with JRE ([#8195]()https://github.com/opensearch-project/OpenSearch/pull/8195)
- Prioritize replica shard movement during shard relocation ([#8875](https://github.com/opensearch-project/OpenSearch/pull/8875))
- Introducing Default and Best Compression codecs as their algorithm name ([#9123]()https://github.com/opensearch-project/OpenSearch/pull/9123)
- Make SearchTemplateRequest implement IndicesRequest.Replaceable ([#9122]()https://github.com/opensearch-project/OpenSearch/pull/9122)
- [BWC and API enforcement] Define the initial set of annotations, their meaning and relations between them ([#9223](https://github.com/opensearch-project/OpenSearch/pull/9223))
- [Remote Store] Add Segment download stats to remotestore stats API ([#8718](https://github.com/opensearch-project/OpenSearch/pull/8718))
- [Remote Store] Add remote segment transfer stats on NodesStats API ([#9168](https://github.com/opensearch-project/OpenSearch/pull/9168))
- [Segment Replication] Support realtime reads for GET requests ([#9212](https://github.com/opensearch-project/OpenSearch/pull/9212))
- Allow test clusters to run with TLS ([#8900](https://github.com/opensearch-project/OpenSearch/pull/8900))
- Add jdk.incubator.vector module support for JDK 20+ ([#8601](https://github.com/opensearch-project/OpenSearch/pull/8601))
- [Feature] Expose term frequency in Painless script score context ([#9081](https://github.com/opensearch-project/OpenSearch/pull/9081))
- Add support for reading partial files to HDFS repository ([#9513](https://github.com/opensearch-project/OpenSearch/issues/9513))

### Dependencies
- Bump `org.apache.logging.log4j:log4j-core` from 2.17.1 to 2.20.0 ([#8307](https://github.com/opensearch-project/OpenSearch/pull/8307))
- Bump `io.grpc:grpc-context` from 1.46.0 to 1.57.1 ([#8726](https://github.com/opensearch-project/OpenSearch/pull/8726), [#9145](https://github.com/opensearch-project/OpenSearch/pull/9145))
- Bump `com.netflix.nebula:gradle-info-plugin` from 12.1.5 to 12.1.6 ([#8724](https://github.com/opensearch-project/OpenSearch/pull/8724))
- Bump `commons-codec:commons-codec` from 1.15 to 1.16.0 ([#8725](https://github.com/opensearch-project/OpenSearch/pull/8725))
- Bump `org.apache.zookeeper:zookeeper` from 3.8.1 to 3.9.0 ([#8844](https://github.com/opensearch-project/OpenSearch/pull/8844), [#9146](https://github.com/opensearch-project/OpenSearch/pull/9146))
- Bump `org.gradle.test-retry` from 1.5.3 to 1.5.4 ([#8842](https://github.com/opensearch-project/OpenSearch/pull/8842))
- Bump `com.netflix.nebula.ospackage-base` from 11.3.0 to 11.4.0 ([#8838](https://github.com/opensearch-project/OpenSearch/pull/8838))
- Bump `com.google.http-client:google-http-client-gson` from 1.43.2 to 1.43.3 ([#8840](https://github.com/opensearch-project/OpenSearch/pull/8840))
- OpenJDK Update (July 2023 Patch releases) ([#8869](https://github.com/opensearch-project/OpenSearch/pull/8869))
- Bump `hadoop` libraries from 3.3.4 to 3.3.6 ([#6995](https://github.com/opensearch-project/OpenSearch/pull/6995))
- Bump `com.gradle.enterprise` from 3.13.3 to 3.14.1 ([#8996](https://github.com/opensearch-project/OpenSearch/pull/8996))
- Bump `org.apache.commons:commons-lang3` from 3.12.0 to 3.13.0 ([#8995](https://github.com/opensearch-project/OpenSearch/pull/8995))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.0 to 2.21.1 ([#8999](https://github.com/opensearch-project/OpenSearch/pull/8999))
- Bump `com.maxmind.geoip2:geoip2` from 4.0.1 to 4.1.0 ([#8998](https://github.com/opensearch-project/OpenSearch/pull/8998))
- Bump `org.apache.commons:commons-lang3` from 3.12.0 to 3.13.0 in /plugins/repository-hdfs ([#8997](https://github.com/opensearch-project/OpenSearch/pull/8997))
- Bump `netty` from 4.1.94.Final to 4.1.96.Final ([#9030](https://github.com/opensearch-project/OpenSearch/pull/9030))
- Bump `com.google.jimfs:jimfs` from 1.2 to 1.3.0 ([#9080](https://github.com/opensearch-project/OpenSearch/pull/9080))
- Bump `io.projectreactor.netty:reactor-netty-http` from 1.1.8 to 1.1.9 ([#9147](https://github.com/opensearch-project/OpenSearch/pull/9147))
- Bump `org.apache.maven:maven-model` from 3.9.3 to 3.9.4 ([#9148](https://github.com/opensearch-project/OpenSearch/pull/9148))
- Bump `com.azure:azure-storage-blob` from 12.22.3 to 12.23.0 ([#9231](https://github.com/opensearch-project/OpenSearch/pull/9231))
- Bump `com.diffplug.spotless` from 6.19.0 to 6.20.0 ([#9227](https://github.com/opensearch-project/OpenSearch/pull/9227))
- Bump `org.xerial.snappy:snappy-java` from 1.1.8.2 to 1.1.10.3 ([#9252](https://github.com/opensearch-project/OpenSearch/pull/9252))
- Bump `com.squareup.okhttp3:okhttp` from 4.9.3 to 4.11.0 ([#9252](https://github.com/opensearch-project/OpenSearch/pull/9252))
- Bump `com.squareup.okio:okio` from 2.8.0 to 3.5.0 ([#9252](https://github.com/opensearch-project/OpenSearch/pull/9252))
- Bump `com.google.code.gson:gson` from 2.9.0 to 2.10.1 ([#9230](https://github.com/opensearch-project/OpenSearch/pull/9230))
- Bump `lycheeverse/lychee-action` from 1.2.0 to 1.8.0 ([#9228](https://github.com/opensearch-project/OpenSearch/pull/9228))
- Bump `snakeyaml` from 2.0 to 2.1 ([#9269](https://github.com/opensearch-project/OpenSearch/pull/9269))
- Bump `aws-actions/configure-aws-credentials` from 1 to 2 ([#9302](https://github.com/opensearch-project/OpenSearch/pull/9302))
- Bump `com.github.luben:zstd-jni` from 1.5.5-3 to 1.5.5-5 ([#9431](https://github.com/opensearch-project/OpenSearch/pull/9431)

### Changed
- Default to mmapfs within hybridfs ([#8508](https://github.com/opensearch-project/OpenSearch/pull/8508))
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))
- Make Span exporter configurable ([#8620](https://github.com/opensearch-project/OpenSearch/issues/8620))
- Perform aggregation postCollection in ContextIndexSearcher after searching leaves ([#8303](https://github.com/opensearch-project/OpenSearch/pull/8303))
- [Refactor] StreamIO from common to core.common namespace in core lib ([#8157](https://github.com/opensearch-project/OpenSearch/pull/8157))
- [Refactor] Remaining HPPC to java.util collections ([#8730](https://github.com/opensearch-project/OpenSearch/pull/8730))
- Remote Segment Store Repository setting moved from `index.remote_store.repository` to `index.remote_store.segment.repository` and `cluster.remote_store.repository` to `cluster.remote_store.segment.repository` respectively for Index and Cluster level settings ([#8719](https://github.com/opensearch-project/OpenSearch/pull/8719))
- Change InternalSignificantTerms to sum shard-level superset counts only in final reduce ([#8735](https://github.com/opensearch-project/OpenSearch/pull/8735))
- Exclude 'benchmarks' from codecov report ([#8805](https://github.com/opensearch-project/OpenSearch/pull/8805))
- Create separate SourceLookup instance per segment slice in SignificantTextAggregatorFactory ([#8807](https://github.com/opensearch-project/OpenSearch/pull/8807))
- Replace the deprecated IndexReader APIs with new storedFields() & termVectors() ([#7792](https://github.com/opensearch-project/OpenSearch/pull/7792))
- [Remote Store] Add support to restore only unassigned shards of an index ([#8792](https://github.com/opensearch-project/OpenSearch/pull/8792))
- Add safeguard limits for file cache during node level allocation ([#8208](https://github.com/opensearch-project/OpenSearch/pull/8208))
- Add support for aggregation profiler with concurrent aggregation ([#8801](https://github.com/opensearch-project/OpenSearch/pull/8801))
- [Remove] Deprecated Fractional ByteSizeValue support #9005 ([#9005](https://github.com/opensearch-project/OpenSearch/pull/9005))
- Add support for aggregation profiler with concurrent aggregation ([#8801](https://github.com/opensearch-project/OpenSearch/pull/8801))
- [Remote Store] Restrict user override for remote store index level settings ([#8812](https://github.com/opensearch-project/OpenSearch/pull/8812))
- [Refactor] MediaTypeParser to MediaTypeParserRegistry ([#8636](https://github.com/opensearch-project/OpenSearch/pull/8636))
- Make MultiBucketConsumerService thread safe to use across slices during search ([#9047](https://github.com/opensearch-project/OpenSearch/pull/9047))
- Removed blocking wait in TransportGetSnapshotsAction which was exhausting generic threadpool ([#8377](https://github.com/opensearch-project/OpenSearch/pull/8377))
- Adds support for tracing runnable scenarios ([#8831](https://github.com/opensearch-project/OpenSearch/pull/8831))
- Change shard_size and shard_min_doc_count evaluation to happen in shard level reduce phase ([#9085](https://github.com/opensearch-project/OpenSearch/pull/9085))
- Add attributes to startSpan methods ([#9199](https://github.com/opensearch-project/OpenSearch/pull/9199))
- [Refactor] Task foundation classes to core library - pt 1 ([#9082](https://github.com/opensearch-project/OpenSearch/pull/9082))
- Add support for wrapping CollectorManager with profiling during concurrent execution ([#9129](https://github.com/opensearch-project/OpenSearch/pull/9129))
- Add base class for parameterizing the search based tests #9083 ([#9083](https://github.com/opensearch-project/OpenSearch/pull/9083))
- Add support for wrapping CollectorManager with profiling during concurrent execution ([#9129](https://github.com/opensearch-project/OpenSearch/pull/9129))
- Rethrow OpenSearch exception for non-concurrent path while using concurrent search ([#9177](https://github.com/opensearch-project/OpenSearch/pull/9177))
- Improve performance of encoding composite keys in multi-term aggregations ([#9412](https://github.com/opensearch-project/OpenSearch/pull/9412))
- Refactor Compressors from CompressorFactory to CompressorRegistry for extensibility ([#9262](https://github.com/opensearch-project/OpenSearch/pull/9262))
- Fix sort related ITs for concurrent search ([#9177](https://github.com/opensearch-project/OpenSearch/pull/9466)

### Deprecated

### Removed
- Remove provision to create Remote Indices without Remote Translog Store ([#8719](https://github.com/opensearch-project/OpenSearch/pull/8719))

### Fixed
- Fix flaky ResourceAwareTasksTests.testBasicTaskResourceTracking test ([#8993](https://github.com/opensearch-project/OpenSearch/pull/8993))
- Fix memory leak when using Zstd Dictionary ([#9403](https://github.com/opensearch-project/OpenSearch/pull/9403))
- Fix condition to remove index create block ([#9437](https://github.com/opensearch-project/OpenSearch/pull/9437))
- Fix range reads in respository-s3 ([9512](https://github.com/opensearch-project/OpenSearch/issues/9512))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.10...2.x
