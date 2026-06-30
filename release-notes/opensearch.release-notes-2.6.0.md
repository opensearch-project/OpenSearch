## 2023-02-22 Version 2.6.0 Release Notes

## [2.6]
### Added
- Add index create block when all nodes have breached high disk watermark ([#5852](https://github.com/opensearch-project/OpenSearch/pull/5852))
- Add cluster manager throttling stats in nodes/stats API ([#5790](https://github.com/opensearch-project/OpenSearch/pull/5790))
- Add support for feature flags in opensearch.yml ([#4959](https://github.com/opensearch-project/OpenSearch/pull/4959))
- Add query for initialized extensions ([#5658](https://github.com/opensearch-project/OpenSearch/pull/5658))
- Add update-index-settings allowlist for searchable snapshot ([#5907](https://github.com/opensearch-project/OpenSearch/pull/5907))
- Add new cat/segment_replication API to surface Segment Replication metrics ([#5718](https://github.com/opensearch-project/OpenSearch/pull/5718)).
- Replace latches with CompletableFutures for extensions ([#5646](https://github.com/opensearch-project/OpenSearch/pull/5646))
- Add support to disallow search request with preference parameter with strict weighted shard routing([#5874](https://github.com/opensearch-project/OpenSearch/pull/5874))
- Add support to apply index create block ([#4603](https://github.com/opensearch-project/OpenSearch/issues/4603))
- Add support for minimum compatible version for extensions ([#6003](https://github.com/opensearch-project/OpenSearch/pull/6003))
- Add a guardrail to limit maximum number of shard on the cluster ([#6143](https://github.com/opensearch-project/OpenSearch/pull/6143))
- Add cancellation of in-flight SearchTasks based on resource consumption ([#5606](https://github.com/opensearch-project/OpenSearch/pull/5605))
- Add support for ppc64le architecture ([#5459](https://github.com/opensearch-project/OpenSearch/pull/5459))
- Add a setting to control auto release of OpenSearch managed index creation block ([#6277](https://github.com/opensearch-project/OpenSearch/pull/6277))
- Fix timeout error when adding a document to an index with extension running ([#6275](https://github.com/opensearch-project/OpenSearch/pull/6275))
- Handle translog upload during primary relocation for remote-backed indexes ([#5804](https://github.com/opensearch-project/OpenSearch/pull/5804))
- Batch translog sync/upload per x ms for remote-backed indexes ([#5854](https://github.com/opensearch-project/OpenSearch/pull/5854))

### Dependencies
- Update nebula-publishing-plugin to 19.2.0 ([#5704](https://github.com/opensearch-project/OpenSearch/pull/5704))
- Bump `reactor-netty` from 1.1.1 to 1.1.2 ([#5878](https://github.com/opensearch-project/OpenSearch/pull/5878))
- OpenJDK Update (January 2023 Patch releases) ([#6075](https://github.com/opensearch-project/OpenSearch/pull/6075))
- Bump `Mockito` from 4.7.0 to 5.1.0, `ByteBuddy` from 1.12.18 to 1.12.22 ([#6088](https://github.com/opensearch-project/OpenSearch/pull/6088))
- Bump `joda` from 2.10.13 to 2.12.2 ([#6095](https://github.com/opensearch-project/OpenSearch/pull/6095))
- Upgrade to Lucene 9.5.0 ([#6078](https://github.com/opensearch-project/OpenSearch/pull/6078))
- Bump antlr4 from 4.9.3 to 4.11.1 ([#6116](https://github.com/opensearch-project/OpenSearch/pull/6116))
- Bump `Netty` from 4.1.86.Final to 4.1.87.Final ([#6130](https://github.com/opensearch-project/OpenSearch/pull/6130))
- Bump `Jackson` from 2.14.1 to 2.14.2 ([#6129](https://github.com/opensearch-project/OpenSearch/pull/6129))
- Bump `org.apache.ant:ant` from 1.10.12 to 1.10.13 ([#6306](https://github.com/opensearch-project/OpenSearch/pull/6306))
- Bump `azure-core-http-netty` from 1.12.7 to 1.12.8 ([#6304](https://github.com/opensearch-project/OpenSearch/pull/6304))
- Bump `com.azure:azure-storage-common` from 12.18.1 to 12.19.3 ([#6304](https://github.com/opensearch-project/OpenSearch/pull/6304))
- Bump `reactor-netty-http` from 1.0.24 to 1.1.2 ([#6309](https://github.com/opensearch-project/OpenSearch/pull/6309))
- Bump `com.google.protobuf:protobuf-java` from 3.21.12 to 3.22.0 ([#6387](https://github.com/opensearch-project/OpenSearch/pull/6387))
- Bump `io.projectreactor.netty:reactor-netty` from 1.1.2 to 1.1.3 ([#6386](https://github.com/opensearch-project/OpenSearch/pull/6386))
- Bump `com.diffplug.spotless` from 6.10.0 to 6.15.0 ([#6385](https://github.com/opensearch-project/OpenSearch/pull/6385))
- Bump `jettison` from 1.5.1 to 1.5.3 ([#6391](https://github.com/opensearch-project/OpenSearch/pull/6391))

### Changed
- Use ReplicationFailedException instead of OpensearchException in ReplicationTarget ([#4725](https://github.com/opensearch-project/OpenSearch/pull/4725))
- [Refactor] Use local opensearch.common.SetOnce instead of lucene's utility class ([#5947](https://github.com/opensearch-project/OpenSearch/pull/5947))
- Cluster health call to throw decommissioned exception for local decommissioned node([#6008](https://github.com/opensearch-project/OpenSearch/pull/6008))
- [Refactor] core.common to new opensearch-common library ([#5976](https://github.com/opensearch-project/OpenSearch/pull/5976))
- Update API spec for cluster health API ([#6399](https://github.com/opensearch-project/OpenSearch/pull/6399))

### Removed
- Remove deprecated org.gradle.util.DistributionLocator usage ([#6212](https://github.com/opensearch-project/OpenSearch/pull/6212))

### Fixed
- [Segment Replication] Fix for peer recovery ([#5344](https://github.com/opensearch-project/OpenSearch/pull/5344))
- Fix weighted shard routing state across search requests([#6004](https://github.com/opensearch-project/OpenSearch/pull/6004))
- [Segment Replication] Fix bug where inaccurate sequence numbers are sent during replication ([#6122](https://github.com/opensearch-project/OpenSearch/pull/6122))
- Enable numeric sort optimisation for few numerical sort types ([#6321](https://github.com/opensearch-project/OpenSearch/pull/6321))
- Fix Opensearch repository-s3 plugin cannot read ServiceAccount token ([#6390](https://github.com/opensearch-project/OpenSearch/pull/6390)
