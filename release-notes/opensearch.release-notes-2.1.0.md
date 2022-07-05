## 2022-07-05 Version 2.1.0 Release Notes


### Breaking Changes in 2.1.0

#### Remove Mapping types

* [Type removal] Ignore _type field in bulk request ([#3505](https://github.com/opensearch-project/opensearch/pull/3505))
* [Type removal] Remove type from BulkRequestParser (#3423) ([#3431](https://github.com/opensearch-project/opensearch/pull/3431))
* [Type removal] _type removal from tests of yaml tests (#3406) ([#3414](https://github.com/opensearch-project/opensearch/pull/3414))

#### Upgrades

* [Upgrade] Lucene-9.3.0-snapshot-b7231bb (#3537) ([#3555](https://github.com/opensearch-project/opensearch/pull/3555))
* [Upgrade] Lucene-9.2.0-snapshot-ba8c3a8 (#3416) ([#3422](https://github.com/opensearch-project/opensearch/pull/3422))
* [Upgrade] Lucene-9.2-snapshot (#2924) ([#3419](https://github.com/opensearch-project/opensearch/pull/3419))


#### Deprecations

* [2.x] Deprecate public methods and variables with master term in package 'org.opensearch.action.support.master' (#3617) ([#3643](https://github.com/opensearch-project/opensearch/pull/3643))
* Deprecate classes in org.opensearch.action.support.master (#3593) ([#3609](https://github.com/opensearch-project/opensearch/pull/3609))

### Security Fixes

* [Dependency upgrade] google-oauth-client to 1.33.3 (#3500) ([#3501](https://github.com/opensearch-project/opensearch/pull/3501))

### Features/Enhancements

* Restore renaming method onMaster() and offMaster() in interface LocalNodeMasterListener ([#3687](https://github.com/opensearch-project/opensearch/pull/3687))
* Revert renaming masterOperation() to clusterManagerOperation() ([#3682](https://github.com/opensearch-project/opensearch/pull/3682))
* Restore AcknowledgedResponse and AcknowledgedRequest to package org.opensearch.action.support.master (#3669) ([#3670](https://github.com/opensearch-project/opensearch/pull/3670))
* Remove class org.opensearch.action.support.master.AcknowledgedResponse (#3662) ([#3668](https://github.com/opensearch-project/opensearch/pull/3668))
* Rename package 'o.o.action.support.master' to 'o.o.action.support.clustermanager' (#3556) ([#3597](https://github.com/opensearch-project/opensearch/pull/3597))
* Support dynamic node role (#3436) ([#3585](https://github.com/opensearch-project/opensearch/pull/3585))
* Support use of IRSA for repository-s3 plugin credentials (#3475) ([#3486](https://github.com/opensearch-project/opensearch/pull/3486))
* Filter out invalid URI and HTTP method in the error message of no handler found for a REST request (#3459) ([#3485](https://github.com/opensearch-project/opensearch/pull/3485))
* Set term vector flags to false for ._index_prefix field (#1901). (#3119) ([#3447](https://github.com/opensearch-project/opensearch/pull/3447))
* Replace internal usages of 'master' term in 'server/src/test' directory (#2520) ([#3444](https://github.com/opensearch-project/opensearch/pull/3444))
* Rename master to cluster_manager in the XContent Parser of ClusterHealthResponse (#3432) ([#3438](https://github.com/opensearch-project/opensearch/pull/3438))
* Replace internal usages of 'master' term in 'server/src/internalClusterTest' directory (#2521) ([#3407](https://github.com/opensearch-project/opensearch/pull/3407))


### Bug Fixes

* [BUG] Custom POM configuration for ZIP publication produces duplicit tags (url, scm) (#3656) ([#3680](https://github.com/opensearch-project/opensearch/pull/3680))
* [BUG] opensearch crashes on closed client connection before search reply (#3626) ([#3645](https://github.com/opensearch-project/opensearch/pull/3645))
* Fix false positive query timeouts due to using cached time (#3454) ([#3624](https://github.com/opensearch-project/opensearch/pull/3624))
* Fix NPE when minBound/maxBound is not set before being called. (#3605) ([#3610](https://github.com/opensearch-project/opensearch/pull/3610))
* Fix for bug showing incorrect awareness attributes count in AwarenessAllocationDecider (#3428) ([#3580](https://github.com/opensearch-project/opensearch/pull/3580))
* Add flat_skew setting to node overload decider (#3563) ([#3582](https://github.com/opensearch-project/opensearch/pull/3582))
* Revert beat types changes 2x  ([#3560](https://github.com/opensearch-project/opensearch/pull/3560))
* Revert "[Remove] MainResponse version override cluster setting (#3031) (#3032)" ([#3530](https://github.com/opensearch-project/opensearch/pull/3530))
* Revert removal of typed end-points for bulk, search, index APIs ([#3524](https://github.com/opensearch-project/opensearch/pull/3524))
* Fix the support of RestClient Node Sniffer for version 2.x and update tests (#3487) ([#3521](https://github.com/opensearch-project/opensearch/pull/3521))
* move bash flag to set statement (#3494) ([#3519](https://github.com/opensearch-project/opensearch/pull/3519))
* [BUG] Fixing org.opensearch.monitor.os.OsProbeTests > testLogWarnCpuMessageOnlyOnes when cgroups are available but cgroup stats is not (#3448) ([#3464](https://github.com/opensearch-project/opensearch/pull/3464))
* Removing unused method from TransportSearchAction (#3437) ([#3445](https://github.com/opensearch-project/opensearch/pull/3445))
* Fix Lucene-snapshots repo for jdk 17. (#3396) ([#3404](https://github.com/opensearch-project/opensearch/pull/3404))

### Build & Infrastructure

* Move gradle-check code to its own scripts and upload codecov (#3742) ([#3747](https://github.com/opensearch-project/opensearch/pull/3747))
* Optimize Gradle builds by enabling local build caching (#3718) ([#3737](https://github.com/opensearch-project/opensearch/pull/3737))
* Update github action gradle-check to use pull_request_target for accessing token (#3728) ([#3731](https://github.com/opensearch-project/opensearch/pull/3731))
* Add gradle check test for github workflows (#3717) ([#3723](https://github.com/opensearch-project/opensearch/pull/3723))

### Maintenance

* Lock 2.1 release to Lucene 9.2.0. ([#3676](https://github.com/opensearch-project/opensearch/pull/3676))
* Added bwc version 2.0.2 ([#3612](https://github.com/opensearch-project/opensearch/pull/3612))
* Added bwc version 1.3.4 (#3551) ([#3583](https://github.com/opensearch-project/opensearch/pull/3583))
* Bump guava from 18.0 to 23.0 in /plugins/ingest-attachment (#3357) ([#3534](https://github.com/opensearch-project/opensearch/pull/3534))
* Added bwc version 2.0.1 ([#3451](https://github.com/opensearch-project/opensearch/pull/3451))
* Bump google-auth-library-oauth2-http from 0.20.0 to 1.7.0 in /plugins/repository-gcs (#3473) ([#3488](https://github.com/opensearch-project/opensearch/pull/3488))
* Bump protobuf-java from 3.20.1 to 3.21.1 in /plugins/repository-hdfs (#3472) ([#3480](https://github.com/opensearch-project/opensearch/pull/3480))
* Bump reactor-core from 3.4.17 to 3.4.18 in /plugins/repository-azure (#3427) ([#3430](https://github.com/opensearch-project/opensearch/pull/3430))
* Bump com.gradle.enterprise from 3.10 to 3.10.1 (#3425) ([#3429](https://github.com/opensearch-project/opensearch/pull/3429))

### Tests

* Fixing flakiness of ShuffleForcedMergePolicyTests (#3591) ([#3592](https://github.com/opensearch-project/opensearch/pull/3592))
* Support use of IRSA for repository-s3 plugin credentials: added YAML Rest test case (#3499) ([#3520](https://github.com/opensearch-project/opensearch/pull/3520))
* Fix testSetAdditionalRolesCanAddDeprecatedMasterRole() by removing the initial assertion (#3441) ([#3443](https://github.com/opensearch-project/opensearch/pull/3443))
