## 2022-09-08 Version 2.3.0 Release Notes

### Features/Enhancements
* [Backport to 2.x] [Segment Replication] - Update replicas to commit SegmentInfos instead of relying on segments_N from primary shards. ([#4450](https://github.com/opensearch-project/opensearch/pull/4450))
* [Segment Replication] [Backport] Fix timeout issue by calculating time needed to process getSegmentFiles. ([#4434](https://github.com/opensearch-project/opensearch/pull/4434))
* [Semgnet Replication] Update flaky testOnNewCheckpointFromNewPrimaryCancelOngoingReplication unit test ([#4414](https://github.com/opensearch-project/opensearch/pull/4414)) ([#4425](https://github.com/opensearch-project/opensearch/pull/4425))
* [Segment Replication] Extend FileChunkWriter to allow cancel on transport client ([#4386](https://github.com/opensearch-project/opensearch/pull/4386)) ([#4424](https://github.com/opensearch-project/opensearch/pull/4424))
* Segment Replication - Fix NoSuchFileException errors caused when computing metadata snapshot on primary shards. ([#4366](https://github.com/opensearch-project/opensearch/pull/4366)) ([#4422](https://github.com/opensearch-project/opensearch/pull/4422))
* [Remote Store] Add index specific setting for remote repository ([#4253](https://github.com/opensearch-project/opensearch/pull/4253)) ([#4418](https://github.com/opensearch-project/opensearch/pull/4418))
* [Segment Replication] Add check to cancel ongoing replication with old primary on onNewCheckpoint on replica ([#4363](https://github.com/opensearch-project/opensearch/pull/4363)) ([#4396](https://github.com/opensearch-project/opensearch/pull/4396))
* [Segment Replication] Bump segment infos counter before commit during replica promotion ([#4365](https://github.com/opensearch-project/opensearch/pull/4365)) ([#4397](https://github.com/opensearch-project/opensearch/pull/4397))
* Segment Replication - Implement segment replication event cancellation. ([#4225](https://github.com/opensearch-project/opensearch/pull/4225)) ([#4387](https://github.com/opensearch-project/opensearch/pull/4387))
* [Backport 2.x] [Remote Store] Backport remote segment store changes ([#4380](https://github.com/opensearch-project/opensearch/pull/4380))
* [Backport 2.x] Added timing data and more granular stages to SegmentReplicationState ([#4367](https://github.com/opensearch-project/opensearch/pull/4367))
* [Backport 2.x] Support shard promotion with Segment Replication. ([#4135](https://github.com/opensearch-project/opensearch/pull/4135)) ([#4325](https://github.com/opensearch-project/opensearch/pull/4325))
* [Segment Replication] Update PrimaryShardAllocator to prefer replicas with higher replication checkpoint ([#4041](https://github.com/opensearch-project/opensearch/pull/4041)) ([#4252](https://github.com/opensearch-project/opensearch/pull/4252))
* [Backport 2.x] [Segment Replication] Backport all PR's containing remaining segment replication changes ([#4243](https://github.com/opensearch-project/opensearch/pull/4243))
* [Backport 2.x] [Segment Replication]  Backport PR's : #3525  #3533  #3540 #3943 #3963 From main branch ([#4181](https://github.com/opensearch-project/opensearch/pull/4181))
* [Backport 2.x] [Segment Replication] Added source-side classes for orchestrating replication events. ([#4128](https://github.com/opensearch-project/opensearch/pull/4128))

### Bug Fixes
*  [Bug]: gradle check failing with java heap OutOfMemoryError ([#4328](https://github.com/opensearch-project/opensearch/pull/4328)) ([#4442](https://github.com/opensearch-project/opensearch/pull/4442))
* [Backport 2.x] Revert to Netty 4.1.79.Final ([#4432](https://github.com/opensearch-project/opensearch/pull/4432))
* Bug fixes for dependabot changelog verifier ([#4364](https://github.com/opensearch-project/opensearch/pull/4364)) ([#4395](https://github.com/opensearch-project/opensearch/pull/4395))
* [BUG] Create logs directory before running OpenSearch on Windows ([#4305](https://github.com/opensearch-project/opensearch/pull/4305)) ([#4335](https://github.com/opensearch-project/opensearch/pull/4335))
* [BUG] Running "opensearch-service.bat start" and "opensearch-service.bat manager" ([#4289](https://github.com/opensearch-project/opensearch/pull/4289)) ([#4293](https://github.com/opensearch-project/opensearch/pull/4293))
* [Backport 2.x] Do not fail replica shard due to primary closure ([#4309](https://github.com/opensearch-project/opensearch/pull/4309))
* [Bug]: gradle check failing with java heap OutOfMemoryError ([#4150](https://github.com/opensearch-project/opensearch/pull/4150)) ([#4167](https://github.com/opensearch-project/opensearch/pull/4167))
* OpenSearch crashes on closed client connection before search reply when total ops higher compared to expected ([#4143](https://github.com/opensearch-project/opensearch/pull/4143)) ([#4144](https://github.com/opensearch-project/opensearch/pull/4144))

### Infrastructure
* Add workflow for changelog verification ([#4085](https://github.com/opensearch-project/opensearch/pull/4085)) ([#4284](https://github.com/opensearch-project/opensearch/pull/4284))
* Add 2.x version to CHANGELOG ([#4297](https://github.com/opensearch-project/opensearch/pull/4297)) ([#4303](https://github.com/opensearch-project/opensearch/pull/4303))
* Update the head ref to changelog verifier ([#4296](https://github.com/opensearch-project/opensearch/pull/4296)) ([#4298](https://github.com/opensearch-project/opensearch/pull/4298))
* Publish transport-netty4 module to central repository ([#4054](https://github.com/opensearch-project/opensearch/pull/4054)) ([#4078](https://github.com/opensearch-project/opensearch/pull/4078))

### Maintenance
* Add bwcVersion 1.3.6 to 2.x  ([#4452](https://github.com/opensearch-project/opensearch/pull/4452))
* [AUTO] [2.x] Added bwc version 2.2.2. ([#4385](https://github.com/opensearch-project/opensearch/pull/4385))
* Update to Netty 4.1.80.Final ([#4359](https://github.com/opensearch-project/opensearch/pull/4359)) ([#4374](https://github.com/opensearch-project/opensearch/pull/4374))
* Adding @dreamer-89 to Opensearch maintainers. ([#4342](https://github.com/opensearch-project/opensearch/pull/4342)) ([#4345](https://github.com/opensearch-project/opensearch/pull/4345))
* [CVE] Update snakeyaml dependency ([#4341](https://github.com/opensearch-project/opensearch/pull/4341)) ([#4347](https://github.com/opensearch-project/opensearch/pull/4347))
* Some dependency updates ([#4308](https://github.com/opensearch-project/opensearch/pull/4308)) ([#4311](https://github.com/opensearch-project/opensearch/pull/4311))
* Added bwc version 2.2.1 ([#4193](https://github.com/opensearch-project/opensearch/pull/4193))
* Update Gradle to 7.5.1 ([#4211](https://github.com/opensearch-project/opensearch/pull/4211)) ([#4213](https://github.com/opensearch-project/opensearch/pull/4213))
* [Backport] Upgrade dependencies ([#4165](https://github.com/opensearch-project/opensearch/pull/4165))
* Bumping 2.x to 2.3.0 ([#4098](https://github.com/opensearch-project/opensearch/pull/4098))

### Refactoring
* Refactored the src and test of GeoHashGrid and GeoTileGrid Aggregations on GeoPoint from server folder to geo module.([#4071](https://github.com/opensearch-project/opensearch/pull/4071)) ([#4072](https://github.com/opensearch-project/opensearch/pull/4072)) ([#4180](https://github.com/opensearch-project/opensearch/pull/4180)) ([#4281](https://github.com/opensearch-project/opensearch/pull/4281))
* Update the head ref to changelog verifier ([#4296](https://github.com/opensearch-project/opensearch/pull/4296)) ([#4298](https://github.com/opensearch-project/opensearch/pull/4298))
* [2.x] Restore using the class ClusterInfoRequest and ClusterInfoRequestBuilder from package 'org.opensearch.action.support.master.info' for subclasses ([#4307](https://github.com/opensearch-project/opensearch/pull/4307)) ([#4324](https://github.com/opensearch-project/opensearch/pull/4324))
* Refactored the src and test of GeoHashGrid and GeoTileGrid Aggregations on GeoPoint from server folder to geo module.([#4071](https://github.com/opensearch-project/opensearch/pull/4071)) ([#4072](https://github.com/opensearch-project/opensearch/pull/4072)) ([#4180](https://github.com/opensearch-project/opensearch/pull/4180)) ([#4281](https://github.com/opensearch-project/opensearch/pull/4281))
* Refactors the GeoBoundsAggregation for geo_point types from the core server to the geo module. ([#4179](https://github.com/opensearch-project/opensearch/pull/4179))
* Backporting multiple 2.* release notes from main to the 2.x branch ([#4154](https://github.com/opensearch-project/opensearch/pull/4154))
