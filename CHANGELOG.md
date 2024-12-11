# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Latency and Memory allocation improvements to Multi Term Aggregation queries ([#14993](https://github.com/opensearch-project/OpenSearch/pull/14993))
- Add support for restoring from snapshot with search replicas ([#16111](https://github.com/opensearch-project/OpenSearch/pull/16111))
- Ensure support of the transport-nio by security plugin ([#16474](https://github.com/opensearch-project/OpenSearch/pull/16474))
- Add logic in master service to optimize performance and retain detailed logging for critical cluster operations. ([#14795](https://github.com/opensearch-project/OpenSearch/pull/14795))
- Add Setting to adjust the primary constraint weights ([#16471](https://github.com/opensearch-project/OpenSearch/pull/16471))
- Switch from `buildSrc/version.properties` to Gradle version catalog (`gradle/libs.versions.toml`) to enable dependabot to perform automated upgrades on common libs ([#16284](https://github.com/opensearch-project/OpenSearch/pull/16284))
- Increase segrep pressure checkpoint default limit to 30 ([#16577](https://github.com/opensearch-project/OpenSearch/pull/16577/files))
- Add dynamic setting allowing size > 0 requests to be cached in the request cache ([#16483](https://github.com/opensearch-project/OpenSearch/pull/16483))
- Support installing plugin SNAPSHOTs with SNASPHOT distribution ([#16581](https://github.com/opensearch-project/OpenSearch/pull/16581))
- Make IndexStoreListener a pluggable interface ([#16583](https://github.com/opensearch-project/OpenSearch/pull/16583))
- Support for keyword fields in star-tree index ([#16233](https://github.com/opensearch-project/OpenSearch/pull/16233))
- Add a flag in QueryShardContext to differentiate inner hit query ([#16600](https://github.com/opensearch-project/OpenSearch/pull/16600))
- Add vertical scaling and SoftReference for snapshot repository data cache ([#16489](https://github.com/opensearch-project/OpenSearch/pull/16489))
- Support prefix list for remote repository attributes([#16271](https://github.com/opensearch-project/OpenSearch/pull/16271))
- Add new configuration setting `synonym_analyzer`, to the `synonym` and `synonym_graph` filters, enabling the specification of a custom analyzer for reading the synonym file ([#16488](https://github.com/opensearch-project/OpenSearch/pull/16488)).
- Add stats for remote publication failure and move download failure stats to remote methods([#16682](https://github.com/opensearch-project/OpenSearch/pull/16682/))
- Added a precaution to handle extreme date values during sorting to prevent `arithmetic_exception: long overflow` ([#16812](https://github.com/opensearch-project/OpenSearch/pull/16812)).

### Dependencies
- Bump `com.google.cloud:google-cloud-core-http` from 2.23.0 to 2.47.0 ([#16504](https://github.com/opensearch-project/OpenSearch/pull/16504))
- Bump `google-auth-library-oauth2-http` from 1.7.0 to 1.29.0 in /plugins/repository-gcs ([#16520](https://github.com/opensearch-project/OpenSearch/pull/16520))
- Bump `com.azure:azure-storage-common` from 12.25.1 to 12.28.0 ([#16521](https://github.com/opensearch-project/OpenSearch/pull/16521), [#16808](https://github.com/opensearch-project/OpenSearch/pull/16808))
- Bump `com.google.apis:google-api-services-compute` from v1-rev20240407-2.0.0 to v1-rev20241105-2.0.0 ([#16502](https://github.com/opensearch-project/OpenSearch/pull/16502), [#16548](https://github.com/opensearch-project/OpenSearch/pull/16548), [#16613](https://github.com/opensearch-project/OpenSearch/pull/16613))
- Bump `com.azure:azure-storage-blob` from 12.23.0 to 12.28.1 ([#16501](https://github.com/opensearch-project/OpenSearch/pull/16501))
- Bump `org.apache.hadoop:hadoop-minicluster` from 3.4.0 to 3.4.1 ([#16550](https://github.com/opensearch-project/OpenSearch/pull/16550))
- Bump `org.apache.xmlbeans:xmlbeans` from 5.2.1 to 5.2.2 ([#16612](https://github.com/opensearch-project/OpenSearch/pull/16612))
- Bump `com.nimbusds:nimbus-jose-jwt` from 9.41.1 to 9.47 ([#16611](https://github.com/opensearch-project/OpenSearch/pull/16611), [#16807](https://github.com/opensearch-project/OpenSearch/pull/16807))
- Bump `lycheeverse/lychee-action` from 2.0.2 to 2.1.0 ([#16610](https://github.com/opensearch-project/OpenSearch/pull/16610))
- Bump `me.champeau.gradle.japicmp` from 0.4.4 to 0.4.5 ([#16614](https://github.com/opensearch-project/OpenSearch/pull/16614))
- Bump `mockito` from 5.14.1 to 5.14.2, `objenesis` from 3.2 to 3.3 and `bytebuddy` from 1.15.4 to 1.15.10 ([#16655](https://github.com/opensearch-project/OpenSearch/pull/16655))
- Bump `Netty` from 4.1.114.Final to 4.1.115.Final ([#16661](https://github.com/opensearch-project/OpenSearch/pull/16661))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.6 to 1.1.10.7 ([#16665](https://github.com/opensearch-project/OpenSearch/pull/16665))
- Bump `codecov/codecov-action` from 4 to 5 ([#16667](https://github.com/opensearch-project/OpenSearch/pull/16667))
- Bump `org.apache.logging.log4j:log4j-core` from 2.24.1 to 2.24.2 ([#16718](https://github.com/opensearch-project/OpenSearch/pull/16718))
- Bump `jackson` from 2.17.2 to 2.18.2 ([#16733](https://github.com/opensearch-project/OpenSearch/pull/16733))
- Bump `ch.qos.logback:logback-classic` from 1.2.13 to 1.5.12 ([#16716](https://github.com/opensearch-project/OpenSearch/pull/16716))
- Bump `com.azure:azure-identity` from 1.13.2 to 1.14.2 ([#16778](https://github.com/opensearch-project/OpenSearch/pull/16778))

### Changed
- Indexed IP field supports `terms_query` with more than 1025 IP masks [#16391](https://github.com/opensearch-project/OpenSearch/pull/16391)
- Make entries for dependencies from server/build.gradle to gradle version catalog ([#16707](https://github.com/opensearch-project/OpenSearch/pull/16707))

### Deprecated
- Performing update operation with default pipeline or final pipeline is deprecated ([#16712](https://github.com/opensearch-project/OpenSearch/pull/16712))

### Removed

### Fixed
- Fix get index settings API doesn't show `number_of_routing_shards` setting when it was explicitly set ([#16294](https://github.com/opensearch-project/OpenSearch/pull/16294))
- Revert changes to upload remote state manifest using minimum codec version([#16403](https://github.com/opensearch-project/OpenSearch/pull/16403))
- Ensure index templates are not applied to system indices ([#16418](https://github.com/opensearch-project/OpenSearch/pull/16418))
- Remove resource usages object from search response headers ([#16532](https://github.com/opensearch-project/OpenSearch/pull/16532))
- Support retrieving doc values of unsigned long field ([#16543](https://github.com/opensearch-project/OpenSearch/pull/16543))
- Fix rollover alias supports restored searchable snapshot index([#16483](https://github.com/opensearch-project/OpenSearch/pull/16483))
- Fix permissions error on scripted query against remote snapshot ([#16544](https://github.com/opensearch-project/OpenSearch/pull/16544))
- Fix `doc_values` only (`index:false`) IP field searching for masks ([#16628](https://github.com/opensearch-project/OpenSearch/pull/16628))
- Fix stale cluster state custom file deletion ([#16670](https://github.com/opensearch-project/OpenSearch/pull/16670))
- [Tiered Caching] Fix bug in cache stats API ([#16560](https://github.com/opensearch-project/OpenSearch/pull/16560))
- Bound the size of cache in deprecation logger ([16702](https://github.com/opensearch-project/OpenSearch/issues/16702))
- Ensure consistency of system flag on IndexMetadata after diff is applied ([#16644](https://github.com/opensearch-project/OpenSearch/pull/16644))
- Skip remote-repositories validations for node-joins when RepositoriesService is not in sync with cluster-state ([#16763](https://github.com/opensearch-project/OpenSearch/pull/16763))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.18...2.x
