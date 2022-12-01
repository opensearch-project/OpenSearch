## 2022-11-30 Version 1.3.6 Release Notes

### Upgrades
* Updated Jackson Databind to 2.13.4.2 ([#4785](https://github.com/opensearch-project/OpenSearch/pull/4785))
* Upgrade netty to 4.1.84.Final ([#4919](https://github.com/opensearch-project/OpenSearch/pull/4919))
* OpenJDK Update (October 2022 Patch releases) ([#5016](https://github.com/opensearch-project/OpenSearch/pull/5016))
* Upgrade com.netflix.nebula:nebula-publishing-plugin to 4.6.0 and gradle-docker-compose-plugin to 0.14.12 ([#5316](https://github.com/opensearch-project/OpenSearch/pull/5136))
* Updated Jackson to 2.14.0 ([#5113](https://github.com/opensearch-project/OpenSearch/pull/5113))
* Updated Jackson to 2.14.1 ([#5356](https://github.com/opensearch-project/OpenSearch/pull/5356))


### Enhancements
* copy build.sh over from opensearch-build ([#4992](https://github.com/opensearch-project/OpenSearch/pull/4992))
* Use BuildParams.isCi() instead of checking env var in build.gradle ([#5368](https://github.com/opensearch-project/OpenSearch/pull/5375))

### Bug Fixes
* Removed depandabot branches during push ([#4110](https://github.com/opensearch-project/OpenSearch/pull/4110))
* Fixed for failing checkExtraction, checkLicense and checkNotice tasks for windows gradle check ([#5119](https://github.com/opensearch-project/OpenSearch/pull/5119))
* Fixed error handling while reading analyzer mapping rules ([#5149](https://github.com/opensearch-project/OpenSearch/pull/5149))
* Fixed testParseWordListOutsideConfigDirError to use a file from another temp directory ([#5158](https://github.com/opensearch-project/OpenSearch/pull/5158))
* Skip SymbolicLinkPreservingTarIT when running on Windows ([#5294](https://github.com/opensearch-project/OpenSearch/pull/5294))
* Fixed flaky test org.opensearch.repositories.s3.RepositoryS3ClientYamlTestSuiteIT/test ([#5339](https://github.com/opensearch-project/OpenSearch/pull/5339))
* Fixed for InternalDistributionArchiveCheckPluginFuncTest failure on Windows ([#5401](https://github.com/opensearch-project/OpenSearch/pull/5401))

