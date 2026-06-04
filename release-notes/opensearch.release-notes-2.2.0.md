## 2022-08-05 Version 2.2.0 Release Notes

### Features/Enhancements

* Task consumer Integration ([#2293](https://github.com/opensearch-project/opensearch/pull/2293)) ([#4141](https://github.com/opensearch-project/opensearch/pull/4141))
* [Backport 2.x] [Segment Replication] Add SegmentReplicationTargetService to orchestrate replication events. ([#4074](https://github.com/opensearch-project/opensearch/pull/4074))
* Support task resource tracking in OpenSearch ([#3982](https://github.com/opensearch-project/opensearch/pull/3982)) ([#4087](https://github.com/opensearch-project/opensearch/pull/4087))
* Making shard copy count a multiple of attribute count ([#3462](https://github.com/opensearch-project/opensearch/pull/3462)) ([#4086](https://github.com/opensearch-project/opensearch/pull/4086))
* [Backport 2.x] [Segment Rreplication] Adding CheckpointRefreshListener to trigger when Segment replication is turned on and Primary shard refreshes ([#4044](https://github.com/opensearch-project/opensearch/pull/4044))
* Add doc_count field mapper ([#3985](https://github.com/opensearch-project/opensearch/pull/3985)) ([#4037](https://github.com/opensearch-project/opensearch/pull/4037))
* Parallelize stale blobs deletion during snapshot delete ([#3796](https://github.com/opensearch-project/opensearch/pull/3796)) ([#3990](https://github.com/opensearch-project/opensearch/pull/3990))
* [Backport 2.x] [Segment Replication] Add a new Engine implementation for replicas with segment replication enabled. ([#4003](https://github.com/opensearch-project/opensearch/pull/4003))
* [Backport 2.x] Adds a new parameter, max_analyzer_offset, for the highlighter ([#4031](https://github.com/opensearch-project/opensearch/pull/4031))
* Update merge on refresh and merge on commit defaults in Opensearch (Lucene 9.3) ([#3561](https://github.com/opensearch-project/opensearch/pull/3561)) ([#4013](https://github.com/opensearch-project/opensearch/pull/4013))
* Make HybridDirectory MMAP Extensions Configurable ([#3837](https://github.com/opensearch-project/opensearch/pull/3837)) ([#3970](https://github.com/opensearch-project/opensearch/pull/3970))
* Add option to disable chunked transfer-encoding ([#3864](https://github.com/opensearch-project/opensearch/pull/3864)) ([#3885](https://github.com/opensearch-project/opensearch/pull/3885))
* Introducing TranslogManager implementations decoupled from the Engine [2.x] ([#3820](https://github.com/opensearch-project/opensearch/pull/3820))
* Changing default  no_master_block from write to metadata_write ([#3621](https://github.com/opensearch-project/opensearch/pull/3621)) ([#3756](https://github.com/opensearch-project/opensearch/pull/3756))

### Bug Fixes

* OpenSearch crashes on closed client connection before search reply when total ops higher compared to expected ([#4143](https://github.com/opensearch-project/opensearch/pull/4143)) ([#4145](https://github.com/opensearch-project/opensearch/pull/4145))
* Binding empty instance of SegmentReplicationCheckpointPublisher when Feature Flag is off in IndicesModule.java file. ([#4119](https://github.com/opensearch-project/opensearch/pull/4119))
* Fix the bug that masterOperation(with task param) is bypassed ([#4103](https://github.com/opensearch-project/opensearch/pull/4103)) ([#4115](https://github.com/opensearch-project/opensearch/pull/4115))
* Fixing flaky org.opensearch.cluster.routing.allocation.decider.DiskThresholdDeciderIT.testHighWatermarkNotExceeded test case ([#4012](https://github.com/opensearch-project/opensearch/pull/4012)) ([#4014](https://github.com/opensearch-project/opensearch/pull/4014))
* Correct typo: Rutime -> Runtime ([#3896](https://github.com/opensearch-project/opensearch/pull/3896)) ([#3898](https://github.com/opensearch-project/opensearch/pull/3898))
* Fixing implausibly old time stamp 1970-01-01 00:00:00 by using the timestamp from the Git revision instead of default 0 value ([#3883](https://github.com/opensearch-project/opensearch/pull/3883)) ([#3891](https://github.com/opensearch-project/opensearch/pull/3891))

### Infrastructure

* Correctly ignore depandabot branches during push ([#4077](https://github.com/opensearch-project/opensearch/pull/4077)) ([#4113](https://github.com/opensearch-project/opensearch/pull/4113))
* Build performance improvements ([#3926](https://github.com/opensearch-project/opensearch/pull/3926)) ([#3937](https://github.com/opensearch-project/opensearch/pull/3937))
* PR coverage requirement and default settings ([#3931](https://github.com/opensearch-project/opensearch/pull/3931)) ([#3938](https://github.com/opensearch-project/opensearch/pull/3938))
* [Backport 2.x] Fail build on wildcard imports ([#3940](https://github.com/opensearch-project/opensearch/pull/3940))
* Don't run EmptyDirTaskTests in a Docker container ([#3792](https://github.com/opensearch-project/opensearch/pull/3792)) ([#3912](https://github.com/opensearch-project/opensearch/pull/3912))
* Add coverage, gha, jenkins server, documentation and forum badges ([#3886](https://github.com/opensearch-project/opensearch/pull/3886))
* Unable to use Systemd module with tar distribution ([#3755](https://github.com/opensearch-project/opensearch/pull/3755)) ([#3903](https://github.com/opensearch-project/opensearch/pull/3903))
* Ignore backport / autocut / dependentbot branches for gradle checks ([#3816](https://github.com/opensearch-project/opensearch/pull/3816)) ([#3825](https://github.com/opensearch-project/opensearch/pull/3825))
* Setup branch push coverage and fix coverage uploads ([#3793](https://github.com/opensearch-project/opensearch/pull/3793)) ([#3811](https://github.com/opensearch-project/opensearch/pull/3811))
* Enable XML test reports for Jenkins integration ([#3799](https://github.com/opensearch-project/opensearch/pull/3799)) ([#3803](https://github.com/opensearch-project/opensearch/pull/3803))

### Maintenance

* OpenJDK Update (July 2022 Patch releases) ([#4023](https://github.com/opensearch-project/opensearch/pull/4023)) ([#4092](https://github.com/opensearch-project/opensearch/pull/4092))
* Update to Lucene 9.3.0 ([#4043](https://github.com/opensearch-project/opensearch/pull/4043)) ([#4088](https://github.com/opensearch-project/opensearch/pull/4088))
* Bump commons-configuration2 from 2.7 to 2.8.0 in /plugins/repository-hdfs ([#3764](https://github.com/opensearch-project/opensearch/pull/3764)) ([#3783](https://github.com/opensearch-project/opensearch/pull/3783))
* Use bash in systemd-entrypoint shebang ([#4008](https://github.com/opensearch-project/opensearch/pull/4008)) ([#4009](https://github.com/opensearch-project/opensearch/pull/4009))
* Bump com.gradle.enterprise from 3.10.1 to 3.10.2 ([#3568](https://github.com/opensearch-project/opensearch/pull/3568)) ([#3934](https://github.com/opensearch-project/opensearch/pull/3934))
* Bump log4j-core in /buildSrc/src/testKit/thirdPartyAudit/sample_jars ([#3763](https://github.com/opensearch-project/opensearch/pull/3763)) ([#3784](https://github.com/opensearch-project/opensearch/pull/3784))
* Added bwc version 1.3.5 ([#3911](https://github.com/opensearch-project/opensearch/pull/3911)) ([#3913](https://github.com/opensearch-project/opensearch/pull/3913))
* Update to Gradle 7.5 ([#3594](https://github.com/opensearch-project/opensearch/pull/3594)) ([#3904](https://github.com/opensearch-project/opensearch/pull/3904))
* Update Netty to 4.1.79.Final ([#3868](https://github.com/opensearch-project/opensearch/pull/3868)) ([#3874](https://github.com/opensearch-project/opensearch/pull/3874))
* Upgrade MinIO image version ([#3541](https://github.com/opensearch-project/opensearch/pull/3541)) ([#3867](https://github.com/opensearch-project/opensearch/pull/3867))
* Add netty-transport-native-unix-common to modules/transport-netty4/bu… ([#3848](https://github.com/opensearch-project/opensearch/pull/3848)) ([#3853](https://github.com/opensearch-project/opensearch/pull/3853))
* Update outdated dependencies ([#3821](https://github.com/opensearch-project/opensearch/pull/3821)) ([#3854](https://github.com/opensearch-project/opensearch/pull/3854))
* Added bwc version 2.1.1 ([#3806](https://github.com/opensearch-project/opensearch/pull/3806))
* Upgrade netty from 4.1.73.Final to 4.1.78.Final ([#3772](https://github.com/opensearch-project/opensearch/pull/3772)) ([#3778](https://github.com/opensearch-project/opensearch/pull/3778))
* Bump protobuf-java from 3.21.1 to 3.21.2 in /plugins/repository-hdfs ([#3711](https://github.com/opensearch-project/opensearch/pull/3711)) ([#3726](https://github.com/opensearch-project/opensearch/pull/3726))
* Upgrading AWS SDK dependency for native plugins ([#3694](https://github.com/opensearch-project/opensearch/pull/3694)) ([#3701](https://github.com/opensearch-project/opensearch/pull/3701))

### Refactoring

* [Backport 2.x] Changes to encapsulate Translog into TranslogManager ([#4095](https://github.com/opensearch-project/opensearch/pull/4095)) ([#4142](https://github.com/opensearch-project/opensearch/pull/4142))
* Deprecate and rename abstract methods in interfaces that contain 'master' in name ([#4121](https://github.com/opensearch-project/opensearch/pull/4121)) ([#4123](https://github.com/opensearch-project/opensearch/pull/4123))
* [Backport 2.x] Integrate Engine with decoupled Translog interfaces ([#3822](https://github.com/opensearch-project/opensearch/pull/3822))
* Deprecate class FakeThreadPoolMasterService, BlockMasterServiceOnMaster and BusyMasterServiceDisruption ([#4058](https://github.com/opensearch-project/opensearch/pull/4058)) ([#4068](https://github.com/opensearch-project/opensearch/pull/4068))
* Rename classes with name 'MasterService' to 'ClusterManagerService' in directory 'test/framework' ([#4051](https://github.com/opensearch-project/opensearch/pull/4051)) ([#4057](https://github.com/opensearch-project/opensearch/pull/4057))
* Deprecate class 'MasterService' and create alternative class 'ClusterManagerService' ([#4022](https://github.com/opensearch-project/opensearch/pull/4022)) ([#4050](https://github.com/opensearch-project/opensearch/pull/4050))
* Deprecate and Rename abstract methods from 'Master' terminology to 'ClusterManager'. ([#4032](https://github.com/opensearch-project/opensearch/pull/4032)) ([#4048](https://github.com/opensearch-project/opensearch/pull/4048))
* Deprecate public methods and variables that contain 'master' terminology in class 'NoMasterBlockService' and 'MasterService' ([#4006](https://github.com/opensearch-project/opensearch/pull/4006)) ([#4038](https://github.com/opensearch-project/opensearch/pull/4038))
* Deprecate public methods and variables that contain 'master' terminology in 'client' directory ([#3966](https://github.com/opensearch-project/opensearch/pull/3966)) ([#3981](https://github.com/opensearch-project/opensearch/pull/3981))
* [segment replication]Introducing common Replication interfaces for segment replication and recovery code paths ([#3234](https://github.com/opensearch-project/opensearch/pull/3234)) ([#3984](https://github.com/opensearch-project/opensearch/pull/3984))
* Deprecate public methods and variables that contain 'master' terminology in 'test/framework' directory  ([#3978](https://github.com/opensearch-project/opensearch/pull/3978)) ([#3987](https://github.com/opensearch-project/opensearch/pull/3987))
* [Backport 2.x] [Segment Replication] Moving RecoveryState.Index to a top-level class and renaming ([#3971](https://github.com/opensearch-project/opensearch/pull/3971))
* Rename and deprecate public methods that contains 'master' in the name in 'server' directory ([#3647](https://github.com/opensearch-project/opensearch/pull/3647)) ([#3964](https://github.com/opensearch-project/opensearch/pull/3964))
* [2.x] Deprecate public class names with master terminology ([#3871](https://github.com/opensearch-project/opensearch/pull/3871)) ([#3914](https://github.com/opensearch-project/opensearch/pull/3914))
* [Backport 2.x] Rename public classes with 'Master' to 'ClusterManager' ([#3870](https://github.com/opensearch-project/opensearch/pull/3870))
* Revert renaming masterOperation() to clusterManagerOperation() ([#3681](https://github.com/opensearch-project/opensearch/pull/3681)) ([#3714](https://github.com/opensearch-project/opensearch/pull/3714))
* Revert renaming method onMaster() and offMaster() in interface LocalNodeMasterListener ([#3686](https://github.com/opensearch-project/opensearch/pull/3686)) ([#3693](https://github.com/opensearch-project/opensearch/pull/3693))
