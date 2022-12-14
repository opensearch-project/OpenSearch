## 2022-12-13 Version 2.4.1 Release Notes

### Bug Fixes
* Fix 1.x compatibility bug with stored Tasks ([#5412](https://github.com/opensearch-project/opensearch/pull/5412)) ([#5440](https://github.com/opensearch-project/opensearch/pull/5440))
* Use BuildParams.isCi() instead of checking env var ([#5368](https://github.com/opensearch-project/opensearch/pull/5368)) ([#5373](https://github.com/opensearch-project/opensearch/pull/5373))
* [BUG] org.opensearch.repositories.s3.RepositoryS3ClientYamlTestSuiteIT/test {yaml=repository_s3/20_repository_permanent_credentials/Snapshot and Restore with repository-s3 using permanent credentials} flaky ([#5325](https://github.com/opensearch-project/opensearch/pull/5325)) ([#5336](https://github.com/opensearch-project/opensearch/pull/5336))
* [BUG] Gradle Check Failed on Windows due to JDK19 pulling by gradle ([#5188](https://github.com/opensearch-project/opensearch/pull/5188)) ([#5192](https://github.com/opensearch-project/opensearch/pull/5192))
* Fix test to use a file from another temp directory ([#5158](https://github.com/opensearch-project/opensearch/pull/5158)) ([#5163](https://github.com/opensearch-project/opensearch/pull/5163))
* Fix boundary condition in indexing pressure test ([#5168](https://github.com/opensearch-project/opensearch/pull/5168)) ([#5182](https://github.com/opensearch-project/opensearch/pull/5182))
* [Backport 2.x] Fix: org.opensearch.clustermanager.ClusterManagerTaskThrottlingIT is flaky. ([#5153](https://github.com/opensearch-project/opensearch/pull/5153)) ([#5171](https://github.com/opensearch-project/opensearch/pull/5171))
* [Backport 2.4] Raise error on malformed CSV ([#5141](https://github.com/opensearch-project/opensearch/pull/5141))

### Features/Enhancements
* Change the output error message back to use OpenSearchException in the cause chain ([#5081](https://github.com/opensearch-project/opensearch/pull/5081)) ([#5085](https://github.com/opensearch-project/opensearch/pull/5085))
* Revert changes in AbstractPointGeometryFieldMapper ([#5250](https://github.com/opensearch-project/opensearch/pull/5250))
* Add support for skipping changelog ([#5088](https://github.com/opensearch-project/opensearch/pull/5088)) ([#5160](https://github.com/opensearch-project/opensearch/pull/5160))
* [Backport 2.4]Revert "Cluster manager task throttling feature [Final PR] ([#5071](https://github.com/opensearch-project/opensearch/pull/5071))  ([#5203](https://github.com/opensearch-project/opensearch/pull/5203))

### Maintenance
* Update Apache Lucene to 9.4.2 ([#5354](https://github.com/opensearch-project/opensearch/pull/5354)) ([#5361](https://github.com/opensearch-project/opensearch/pull/5361))
* Update Jackson to 2.14.1 ([#5346](https://github.com/opensearch-project/opensearch/pull/5346)) ([#5358](https://github.com/opensearch-project/opensearch/pull/5358))
* Bump nebula-publishing-plugin from v4.4.0 to v4.6.0. ([#5127](https://github.com/opensearch-project/opensearch/pull/5127)) ([#5131](https://github.com/opensearch-project/opensearch/pull/5131))
* Bump commons-compress from 1.21 to 1.22. ([#5520](https://github.com/opensearch-project/OpenSearch/pull/5520)) ([#5522](https://github.com/opensearch-project/opensearch/pull/5522))
