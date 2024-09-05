# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Workload Management] Add Settings for Workload Management feature ([#15028](https://github.com/opensearch-project/OpenSearch/pull/15028))
- Fix for hasInitiatedFetching to fix allocation explain and manual reroute APIs (([#14972](https://github.com/opensearch-project/OpenSearch/pull/14972))
- [Workload Management] Add queryGroupId to Task ([14708](https://github.com/opensearch-project/OpenSearch/pull/14708))
- Add setting to ignore throttling nodes for allocation of unassigned primaries in remote restore ([#14991](https://github.com/opensearch-project/OpenSearch/pull/14991))
- [Workload Management] Add Delete QueryGroup API Logic ([#14735](https://github.com/opensearch-project/OpenSearch/pull/14735))
- [Streaming Indexing] Enhance RestClient with a new streaming API support ([#14437](https://github.com/opensearch-project/OpenSearch/pull/14437))
- Add basic aggregation support for derived fields ([#14618](https://github.com/opensearch-project/OpenSearch/pull/14618))
- [Workload Management] Add Create QueryGroup API Logic ([#14680](https://github.com/opensearch-project/OpenSearch/pull/14680))- [Workload Management] Add Create QueryGroup API Logic ([#14680](https://github.com/opensearch-project/OpenSearch/pull/14680))
- Add ThreadContextPermission for markAsSystemContext and allow core to perform the method ([#15016](https://github.com/opensearch-project/OpenSearch/pull/15016))
- Add ThreadContextPermission for stashAndMergeHeaders and stashWithOrigin ([#15039](https://github.com/opensearch-project/OpenSearch/pull/15039))
- [Concurrent Segment Search] Support composite aggregations with scripting ([#15072](https://github.com/opensearch-project/OpenSearch/pull/15072))
- Add `rangeQuery` and `regexpQuery` for `constant_keyword` field type ([#14711](https://github.com/opensearch-project/OpenSearch/pull/14711))
- Add took time to request nodes stats ([#15054](https://github.com/opensearch-project/OpenSearch/pull/15054))
- [Workload Management] Add Get QueryGroup API Logic ([14709](https://github.com/opensearch-project/OpenSearch/pull/14709))
- [Workload Management] Add Update QueryGroup API Logic ([#14775](https://github.com/opensearch-project/OpenSearch/pull/14775))
- [Workload Management] QueryGroup resource tracking framework changes ([#13897](https://github.com/opensearch-project/OpenSearch/pull/13897))
- Support filtering on a large list encoded by bitmap ([#14774](https://github.com/opensearch-project/OpenSearch/pull/14774))
- Add slice execution listeners to SearchOperationListener interface ([#15153](https://github.com/opensearch-project/OpenSearch/pull/15153))
- Make balanced shards allocator timebound ([#15239](https://github.com/opensearch-project/OpenSearch/pull/15239))
- Add allowlist setting for ingest-geoip and ingest-useragent ([#15325](https://github.com/opensearch-project/OpenSearch/pull/15325))
- Adding access to noSubMatches and noOverlappingMatches in Hyphenation ([#13895](https://github.com/opensearch-project/OpenSearch/pull/13895))
- Star tree mapping changes ([#14605](https://github.com/opensearch-project/OpenSearch/pull/14605))
- Add support for index level max slice count setting for concurrent segment search ([#15336](https://github.com/opensearch-project/OpenSearch/pull/15336))
- Support cancellation for cat shards and node stats API.([#13966](https://github.com/opensearch-project/OpenSearch/pull/13966))
- [Streaming Indexing] Introduce bulk HTTP API streaming flavor ([#15381](https://github.com/opensearch-project/OpenSearch/pull/15381))
- Add support for centralize snapshot creation with pinned timestamp ([#15124](https://github.com/opensearch-project/OpenSearch/pull/15124))
- Add concurrent search support for Derived Fields ([#15326](https://github.com/opensearch-project/OpenSearch/pull/15326))
- [Workload Management] Add query group stats constructs ([#15343](https://github.com/opensearch-project/OpenSearch/pull/15343)))
- Add limit on number of processors for Ingest pipeline([#15460](https://github.com/opensearch-project/OpenSearch/pull/15465)).
- Add runAs to Subject interface and introduce IdentityAwarePlugin extension point ([#14630](https://github.com/opensearch-project/OpenSearch/pull/14630))
- [Workload Management] Add rejection logic for co-ordinator and shard level requests ([#15428](https://github.com/opensearch-project/OpenSearch/pull/15428)))
- Adding translog durability validation in index templates ([#15494](https://github.com/opensearch-project/OpenSearch/pull/15494))
- [Range Queries] Add new approximateable query framework to short-circuit range queries ([#13788](https://github.com/opensearch-project/OpenSearch/pull/13788))
- [Workload Management] Add query group level failure tracking ([#15227](https://github.com/opensearch-project/OpenSearch/pull/15527))
- [Reader Writer Separation] Add experimental search replica shard type to achieve reader writer separation ([#15237](https://github.com/opensearch-project/OpenSearch/pull/15237))
- Add index creation using the context field ([#15290](https://github.com/opensearch-project/OpenSearch/pull/15290))
- [Remote Publication] Add remote download stats ([#15291](https://github.com/opensearch-project/OpenSearch/pull/15291)))
- Add support to upload snapshot shard blobs with hashed prefix ([#15426](https://github.com/opensearch-project/OpenSearch/pull/15426))
- Add prefix support to hashed prefix & infix path types on remote store ([#15557](https://github.com/opensearch-project/OpenSearch/pull/15557))
- Add canRemain method to TargetPoolAllocationDecider to move shards from local to remote pool for hot to warm tiering ([#15010](https://github.com/opensearch-project/OpenSearch/pull/15010))
- Add support for pluggable deciders for concurrent search ([#15363](https://github.com/opensearch-project/OpenSearch/pull/15363))
- Add support for comma-separated list of index names to be used with Snapshot Status API ([#15409](https://github.com/opensearch-project/OpenSearch/pull/15409))[SnapshotV2] Snapshot Status API changes (#15409))
- Add path prefix support to hashed prefix snapshots ([#15664](https://github.com/opensearch-project/OpenSearch/pull/15664))
- Optimise snapshot deletion to speed up snapshot deletion and creation ([#15568](https://github.com/opensearch-project/OpenSearch/pull/15568))
- [Remote Publication] Added checksum validation for cluster state behind a cluster setting ([#15218](https://github.com/opensearch-project/OpenSearch/pull/15218))
- Relax the join validation for Remote State publication ([#15471](https://github.com/opensearch-project/OpenSearch/pull/15471))
- Optimize NodeIndicesStats output behind flag ([#14454](https://github.com/opensearch-project/OpenSearch/pull/14454))
- MultiTermQueries in keyword fields now default to `indexed` approach and gated behind cluster setting ([#15637](https://github.com/opensearch-project/OpenSearch/pull/15637))

### Dependencies
- Bump `netty` from 4.1.111.Final to 4.1.112.Final ([#15081](https://github.com/opensearch-project/OpenSearch/pull/15081))
- Bump `org.apache.commons:commons-lang3` from 3.14.0 to 3.15.0 ([#14861](https://github.com/opensearch-project/OpenSearch/pull/14861))
- OpenJDK Update (July 2024 Patch releases) ([#14998](https://github.com/opensearch-project/OpenSearch/pull/14998))
- Bump `com.microsoft.azure:msal4j` from 1.16.1 to 1.17.0 ([#14995](https://github.com/opensearch-project/OpenSearch/pull/14995), [#15420](https://github.com/opensearch-project/OpenSearch/pull/15420))
- Bump `actions/github-script` from 6 to 7 ([#14997](https://github.com/opensearch-project/OpenSearch/pull/14997))
- Bump `org.tukaani:xz` from 1.9 to 1.10 ([#15110](https://github.com/opensearch-project/OpenSearch/pull/15110))
- Bump `org.apache.avro:avro` from 1.11.3 to 1.12.0 in /plugins/repository-hdfs ([#15119](https://github.com/opensearch-project/OpenSearch/pull/15119))
- Bump `org.bouncycastle:bcpg-fips` from 1.0.7.1 to 2.0.9 ([#15103](https://github.com/opensearch-project/OpenSearch/pull/15103), [#15299](https://github.com/opensearch-project/OpenSearch/pull/15299))
- Bump `com.azure:azure-core` from 1.49.1 to 1.51.0 ([#15111](https://github.com/opensearch-project/OpenSearch/pull/15111))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.5 to 1.1.10.6 ([#15207](https://github.com/opensearch-project/OpenSearch/pull/15207))
- Bump `com.azure:azure-xml` from 1.0.0 to 1.1.0 ([#15206](https://github.com/opensearch-project/OpenSearch/pull/15206))
- Bump `reactor` from 3.5.19 to 3.5.20 ([#15262](https://github.com/opensearch-project/OpenSearch/pull/15262))
- Bump `reactor-netty` from 1.1.21 to 1.1.22 ([#15262](https://github.com/opensearch-project/OpenSearch/pull/15262))
- Bump `org.apache.kerby:kerb-admin` from 2.0.3 to 2.1.0 ([#15301](https://github.com/opensearch-project/OpenSearch/pull/15301))
- Bump `com.azure:azure-core-http-netty` from 1.15.1 to 1.15.3 ([#15300](https://github.com/opensearch-project/OpenSearch/pull/15300))
- Bump `com.gradle.develocity` from 3.17.6 to 3.18 ([#15297](https://github.com/opensearch-project/OpenSearch/pull/15297))
- Bump `commons-cli:commons-cli` from 1.8.0 to 1.9.0 ([#15298](https://github.com/opensearch-project/OpenSearch/pull/15298))
- Bump `opentelemetry` from 1.40.0 to 1.41.0 ([#15361](https://github.com/opensearch-project/OpenSearch/pull/15361))
- Bump `opentelemetry-semconv` from 1.26.0-alpha to 1.27.0-alpha ([#15361](https://github.com/opensearch-project/OpenSearch/pull/15361))
- Bump `tj-actions/changed-files` from 44 to 45 ([#15422](https://github.com/opensearch-project/OpenSearch/pull/15422))
- Bump `dnsjava:dnsjava` from 3.6.0 to 3.6.1 ([#15418](https://github.com/opensearch-project/OpenSearch/pull/15418))
- Bump `com.netflix.nebula.ospackage-base` from 11.9.1 to 11.10.0 ([#15419](https://github.com/opensearch-project/OpenSearch/pull/15419))
- Bump `org.roaringbitmap:RoaringBitmap` from 1.1.0 to 1.2.1 ([#15423](https://github.com/opensearch-project/OpenSearch/pull/15423))
- Bump `icu4j` from 70.1 to 75.1 ([#15469](https://github.com/opensearch-project/OpenSearch/pull/15469))

### Changed
- Add lower limit for primary and replica batch allocators timeout ([#14979](https://github.com/opensearch-project/OpenSearch/pull/14979))
- Optimize regexp-based include/exclude on aggregations when pattern matches prefixes ([#14371](https://github.com/opensearch-project/OpenSearch/pull/14371))
- Replace and block usages of org.apache.logging.log4j.util.Strings ([#15238](https://github.com/opensearch-project/OpenSearch/pull/15238))
- Remote publication using minimum node version for backward compatibility ([#15216](https://github.com/opensearch-project/OpenSearch/pull/15216))


### Deprecated

### Removed
- Remove some unused code in the search backpressure package ([#15518](https://github.com/opensearch-project/OpenSearch/pull/15518))

### Fixed
- Fix constraint bug which allows more primary shards than average primary shards per index ([#14908](https://github.com/opensearch-project/OpenSearch/pull/14908))
- Fix NPE when bulk ingest with empty pipeline ([#15033](https://github.com/opensearch-project/OpenSearch/pull/15033))
- Fix missing value of FieldSort for unsigned_long ([#14963](https://github.com/opensearch-project/OpenSearch/pull/14963))
- Fix delete index template failed when the index template matches a data stream but is unused ([#15080](https://github.com/opensearch-project/OpenSearch/pull/15080))
- Fix array_index_out_of_bounds_exception when indexing documents with field name containing only dot ([#15126](https://github.com/opensearch-project/OpenSearch/pull/15126))
- Fixed array field name omission in flat_object function for nested JSON ([#13620](https://github.com/opensearch-project/OpenSearch/pull/13620))
- Fix incorrect parameter names in MinHash token filter configuration handling ([#15233](https://github.com/opensearch-project/OpenSearch/pull/15233))
- Fix range aggregation optimization ignoring top level queries ([#15287](https://github.com/opensearch-project/OpenSearch/pull/15287))
- Fix indexing error when flat_object field is explicitly null ([#15375](https://github.com/opensearch-project/OpenSearch/pull/15375))
- Fix split response processor not included in allowlist ([#15393](https://github.com/opensearch-project/OpenSearch/pull/15393))
- Fix unchecked cast in dynamic action map getter ([#15394](https://github.com/opensearch-project/OpenSearch/pull/15394))
- Fix null values indexed as "null" strings in flat_object field ([#14069](https://github.com/opensearch-project/OpenSearch/pull/14069))
- Fix terms query on wildcard field returns nothing ([#15607](https://github.com/opensearch-project/OpenSearch/pull/15607))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.16...2.x
