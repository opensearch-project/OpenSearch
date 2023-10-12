## 2023-10-12 Version 2.11.0 Release Notes

## [2.11]

### Added
- Add coordinator level stats for search latency ([#8386](https://github.com/opensearch-project/OpenSearch/issues/8386))
- Add metrics for thread_pool task wait time ([#9681](https://github.com/opensearch-project/OpenSearch/pull/9681))
- Async blob read support for S3 plugin ([#9694](https://github.com/opensearch-project/OpenSearch/pull/9694))
- [Telemetry-Otel] Added support for OtlpGrpcSpanExporter exporter ([#9666](https://github.com/opensearch-project/OpenSearch/pull/9666))
- Async blob read support for encrypted containers ([#10131](https://github.com/opensearch-project/OpenSearch/pull/10131))
- Add capability to restrict async durability mode for remote indexes ([#10189](https://github.com/opensearch-project/OpenSearch/pull/10189))
- Add Doc Status Counter for Indexing Engine ([#4562](https://github.com/opensearch-project/OpenSearch/issues/4562))
- Add unreferenced file cleanup count to merge stats ([#10204](https://github.com/opensearch-project/OpenSearch/pull/10204))
- [Remote Store] Add support to restrict creation & deletion if system repository and mutation of immutable settings of system repository ([#9839](https://github.com/opensearch-project/OpenSearch/pull/9839))
- Improve compressed request handling ([#10261](https://github.com/opensearch-project/OpenSearch/pull/10261))

### Dependencies
- Bump `peter-evans/create-or-update-comment` from 2 to 3 ([#9575](https://github.com/opensearch-project/OpenSearch/pull/9575))
- Bump `actions/checkout` from 2 to 4 ([#9968](https://github.com/opensearch-project/OpenSearch/pull/9968))
- Bump OpenTelemetry from 1.26.0 to 1.30.1 ([#9950](https://github.com/opensearch-project/OpenSearch/pull/9950))
- Bump `org.apache.commons:commons-compress` from 1.23.0 to 1.24.0 ([#9973, #9972](https://github.com/opensearch-project/OpenSearch/pull/9973, https://github.com/opensearch-project/OpenSearch/pull/9972))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.1 to 2.23.0 ([#9971](https://github.com/opensearch-project/OpenSearch/pull/9971))
- Bump `mockito` from 5.4.0 to 5.5.0 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `bytebuddy` from 1.14.3 to 1.14.7 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `com.zaxxer:SparseBitSet` from 1.2 to 1.3 ([#10098](https://github.com/opensearch-project/OpenSearch/pull/10098))
- Bump `tibdex/github-app-token` from 1.5.0 to 2.1.0 ([#10125](https://github.com/opensearch-project/OpenSearch/pull/10125))
- Bump `org.wiremock:wiremock-standalone` from 2.35.0 to 3.1.0 ([#9752](https://github.com/opensearch-project/OpenSearch/pull/9752))
- Bump `com.google.http-client:google-http-client-jackson2` from 1.43.2 to 1.43.3 ([#10126](https://github.com/opensearch-project/OpenSearch/pull/10126))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.3 to 1.1.10.5 ([#10206](https://github.com/opensearch-project/OpenSearch/pull/10206), [#10299](https://github.com/opensearch-project/OpenSearch/pull/10299))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.10.0 to 2.25.1 ([#10208](https://github.com/opensearch-project/OpenSearch/pull/10208), [#10298](https://github.com/opensearch-project/OpenSearch/pull/10298))
- Bump `codecov/codecov-action` from 2 to 3 ([#10209](https://github.com/opensearch-project/OpenSearch/pull/10209))
- Bump `org.bouncycastle:bcpkix-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`
- Bump `org.bouncycastle:bcprov-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`
- Bump `org.bouncycastle:bcmail-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`
- Bump Lucene from 9.7.0 to 9.8.0 ([10276](https://github.com/opensearch-project/OpenSearch/pull/10276))
- Bump `com.netflix.nebula.ospackage-base` from 11.4.0 to 11.5.0 ([#10295](https://github.com/opensearch-project/OpenSearch/pull/10295))
- Bump asm from 9.5 to 9.6 ([#10302](https://github.com/opensearch-project/OpenSearch/pull/10302))
- Bump netty from 4.1.97.Final to 4.1.99.Final ([#10303](https://github.com/opensearch-project/OpenSearch/pull/10303))
- Bump `peter-evans/create-pull-request` from 3 to 5 ([#10301](https://github.com/opensearch-project/OpenSearch/pull/10301))
- Bump `org.apache.avro:avro` from 1.11.2 to 1.11.3 ([#10210](https://github.com/opensearch-project/OpenSearch/pull/10210))
- Bump `org.bouncycastle:bc-fips` from 1.0.2.3 to 1.0.2.4 ([#10297](https://github.com/opensearch-project/OpenSearch/pull/10297))
- Bump `org.apache.zookeeper:zookeeper` from 3.9.0 to 3.9.1 ([#10506](https://github.com/opensearch-project/OpenSearch/pull/10506))
- Bump `de.thetaphi:forbiddenapis` from 3.5.1 to 3.6 ([#10508](https://github.com/opensearch-project/OpenSearch/pull/10508))
- Bump `commons-io:commons-io` from 2.13.0 to 2.14.0 ([#10294](https://github.com/opensearch-project/OpenSearch/pull/10294))
- Bump `netty` from 4.1.99.Final to 4.1.100.Final ([#10564](https://github.com/opensearch-project/OpenSearch/pull/10564))

### Changed
- Add instrumentation in rest and network layer. ([#9415](https://github.com/opensearch-project/OpenSearch/pull/9415))
- Allow parameterization of tests with OpenSearchIntegTestCase.SuiteScopeTestCase annotation ([#9916](https://github.com/opensearch-project/OpenSearch/pull/9916))
- Mute the query profile IT with concurrent execution ([#9840](https://github.com/opensearch-project/OpenSearch/pull/9840))
- Force merge with `only_expunge_deletes` honors max segment size ([#10036](https://github.com/opensearch-project/OpenSearch/pull/10036))
- Add instrumentation in transport service. ([#10042](https://github.com/opensearch-project/OpenSearch/pull/10042))
- [Tracing Framework] Add support for SpanKind. ([#10122](https://github.com/opensearch-project/OpenSearch/pull/10122))
- Pass parent filter to inner query in nested query ([#10246](https://github.com/opensearch-project/OpenSearch/pull/10246))
- Disable concurrent segment search when terminate_after is used ([#10200](https://github.com/opensearch-project/OpenSearch/pull/10200))
- Add instrumentation in Inbound Handler. ([#100143](https://github.com/opensearch-project/OpenSearch/pull/10143))
- Enable remote segment upload backpressure by default ([#10356](https://github.com/opensearch-project/OpenSearch/pull/10356))
- [Remote Store] Add support to reload repository metadata inplace ([#9569](https://github.com/opensearch-project/OpenSearch/pull/9569))
- [Metrics Framework] Add Metrics framework. ([#10241](https://github.com/opensearch-project/OpenSearch/pull/10241))
- Updating the separator for RemoteStoreLockManager since underscore is allowed in base64UUID url charset ([#10379](https://github.com/opensearch-project/OpenSearch/pull/10379))
- Add the means to extract the contextual properties from HttpChannel, TcpCChannel and TrasportChannel without excessive typecasting ([#10562](https://github.com/opensearch-project/OpenSearch/pull/10562))

### Removed
- Remove spurious SGID bit on directories ([#9447](https://github.com/opensearch-project/OpenSearch/pull/9447))

### Fixed
- Fix ignore_missing parameter has no effect when using template snippet in rename ingest processor ([#9725](https://github.com/opensearch-project/OpenSearch/pull/9725))
- Fix broken backward compatibility from 2.7 for IndexSorted field indices ([#10045](https://github.com/opensearch-project/OpenSearch/pull/10045))
- Fix concurrent search NPE when track_total_hits, terminate_after and size=0 are used ([#10082](https://github.com/opensearch-project/OpenSearch/pull/10082))
- Fix remove ingest processor handing ignore_missing parameter not correctly ([10089](https://github.com/opensearch-project/OpenSearch/pull/10089))
- Fix class_cast_exception when passing int to _version and other metadata fields in ingest simulate API ([#10101](https://github.com/opensearch-project/OpenSearch/pull/10101))
- Fix circular dependency in Settings initialization ([10194](https://github.com/opensearch-project/OpenSearch/pull/10194))
- Fix registration and initialization of multiple extensions ([10256](https://github.com/opensearch-project/OpenSearch/pull/10256))
- Fix Segment Replication ShardLockObtainFailedException bug during index corruption ([10370](https://github.com/opensearch-project/OpenSearch/pull/10370))
- Fix some test methods in SimulatePipelineRequestParsingTests never run and fix test failure ([#10496](https://github.com/opensearch-project/OpenSearch/pull/10496))
