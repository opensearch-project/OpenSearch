## Version 3.3.2 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.3.2

### Fixed
* Fix issue with updating core with a patch number other than 0 ([#19377](https://github.com/opensearch-project/OpenSearch/pull/19377))
* [Star Tree] Fix sub-aggregator casting for search with profile=true ([#19652](https://github.com/opensearch-project/OpenSearch/pull/19652))
* Fix bwc @timestamp upgrade issue by adding a version check on skip_list param ([#19671](https://github.com/opensearch-project/OpenSearch/pull/19671))
* [Java Agent] Allow JRT protocol URLs in protection domain extraction ([#19683](https://github.com/opensearch-project/OpenSearch/pull/19683))
* Add S3Repository.LEGACY_MD5_CHECKSUM_CALCULATION to list of repository-s3 settings ([#19789](https://github.com/opensearch-project/OpenSearch/pull/19789))
* Fix IndexOutOfBoundsException when running include/exclude on non-existent prefix in terms aggregations ([#19637](https://github.com/opensearch-project/OpenSearch/pull/19637))

### Dependencies
* Bump ch.qos.logback modules from 1.5.18 to 1.5.20 in HDFS test fixture ([#19764](https://github.com/opensearch-project/OpenSearch/pull/19764))