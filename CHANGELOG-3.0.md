# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679), [#10664](https://github.com/opensearch-project/OpenSearch/pull/10664))
- Provide service accounts tokens to extensions ([#9618](https://github.com/opensearch-project/OpenSearch/pull/9618))
- GHA to verify checklist items completion in PR descriptions ([#10800](https://github.com/opensearch-project/OpenSearch/pull/10800))
- Allow to pass the list settings through environment variables (like [], ["a", "b", "c"], ...) ([#10625](https://github.com/opensearch-project/OpenSearch/pull/10625))
- Views, simplify data access and manipulation by providing a virtual layer over one or more indices ([#11957](https://github.com/opensearch-project/OpenSearch/pull/11957))

### Dependencies

### Changed
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))
- Add task completion count in search backpressure stats API ([#10028](https://github.com/opensearch-project/OpenSearch/pull/10028/))
- Deprecate CamelCase `PathHierarchy` tokenizer name in favor to lowercase `path_hierarchy` ([#10894](https://github.com/opensearch-project/OpenSearch/pull/10894))
- Breaking change: Do not request "search_pipelines" metrics by default in NodesInfoRequest ([#12497](https://github.com/opensearch-project/OpenSearch/pull/12497))
- [Query Sandbox] Add Resource Limit Group CRUD APIs ([#13315](https://github.com/opensearch-project/OpenSearch/pull/13315))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))
- Remove custom Map, List and Set collection classes ([#6871](https://github.com/opensearch-project/OpenSearch/pull/6871))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Don't over-allocate in HeapBufferedAsyncEntityConsumer in order to consume the response ([#9993](https://github.com/opensearch-project/OpenSearch/pull/9993))

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
