# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 5.0.0]
### Added
- Add new users, roles and mappings [(#886)](https://github.com/wazuh/wazuh-indexer/pull/886)
- Add custom GitHub Action to validate commiter's emails by domain [(#896)](https://github.com/wazuh/wazuh-indexer/pull/896)
- Migrate to OpenSearch 3.0.0 [(#903)](https://github.com/wazuh/wazuh-indexer/pull/903)
- Add Wazuh version comparison [(#936)](https://github.com/wazuh/wazuh-indexer/pull/936)
- Include Reporting plugin in Wazuh Indexer by default [(#1008)](https://github.com/wazuh/wazuh-indexer/pull/1008)
- Make Wazuh Indexer roles reserved [(#1012)](https://github.com/wazuh/wazuh-indexer/pull/1012)
- Add Cross-Cluster Search environment [(#1034)](https://github.com/wazuh/wazuh-indexer/pull/1034)
- Add Security Analytics fork to Wazuh Indexer packages [(#1188)](https://github.com/wazuh/wazuh-indexer/pull/1188)
- Map `alerting_full_access` and `notifications_full_access` roles to the `kibanaserver` user [(#1201)](https://github.com/wazuh/wazuh-indexer/pull/1201)
- Create new roles for Indexer Content Manager API [(#1243)](https://github.com/wazuh/wazuh-indexer/pull/1243)
- Add new `cluster.default_number_of_replicas` setting to `opensearch.yml` [(#1292)](https://github.com/wazuh/wazuh-indexer/pull/1292)
- Bundle engine in wazuh-indexer package [(#1298)](https://github.com/wazuh/wazuh-indexer/pull/1298) [(#1302)](https://github.com/wazuh/wazuh-indexer/pull/1302)
- Implement SAP Local Maven publisher GHA [(#1304)](https://github.com/wazuh/wazuh-indexer/pull/1304)
- Enable Wazuh Engine in Docker images and add support for ARM architecture [(#1320)](https://github.com/wazuh/wazuh-indexer/pull/1320)

### Fixed
- Fix Snapshot rename replacement unbounded length rename ([#20464](https://github.com/opensearch-project/OpenSearch/issues/20464))
- Fix segment replication failure during rolling restart ([#19234](https://github.com/opensearch-project/OpenSearch/issues/19234))
- Fix bug of warm index: FullFileCachedIndexInput was closed error ([#20055](https://github.com/opensearch-project/OpenSearch/pull/20055))
- Fix flaky test ClusterMaxMergesAtOnceIT.testClusterLevelDefaultUpdatesMergePolicy ([#18056](https://github.com/opensearch-project/OpenSearch/issues/18056))
- Fix bug in Assertion framework(Yaml Rest test): numeric comparison fails when comparing Integer vs Long (or Float vs Double) ([#19376](https://github.com/opensearch-project/OpenSearch/pull/19376))
- Fix Netty deprecation warnings in transport-netty4 module ([#20233](https://github.com/opensearch-project/OpenSearch/pull/20233))
- Fix snapshot restore when an index sort is present ([#20284](https://github.com/opensearch-project/OpenSearch/pull/20284))
- Fix SearchPhaseExecutionException to properly initCause ([#20320](https://github.com/opensearch-project/OpenSearch/pull/20320))
- [repository-s3] remove endpointOverride and let AWS SDK V2 S3 determine the s3 url based on bucket name or arn provided ([#20345](https://github.com/opensearch-project/OpenSearch/pull/20345))
- Fix `cluster.remote.<cluster_alias>.server_name` setting no populating SNI ([#20321](https://github.com/opensearch-project/OpenSearch/pull/20321))
- Fix X-Opaque-Id header propagation (along with other response headers) for streaming Reactor Netty 4 transport ([#20371](https://github.com/opensearch-project/OpenSearch/pull/20371))
- Allow removing plugin that's optionally extended ([#20417](https://github.com/opensearch-project/OpenSearch/pull/20417))
- Fix indexing regression and bug fixes for grouping criteria. ([20145](https://github.com/opensearch-project/OpenSearch/pull/20145))
- LeafReader should not remove SubReaderWrappers incase IndexWriter encounters a non aborting Exception ([#20193](https://github.com/opensearch-project/OpenSearch/pull/20193))
- Fix Netty deprecation warnings in transport-reactor-netty4 module ([20429](https://github.com/opensearch-project/OpenSearch/pull/20429))
- Fix stats aggregation returning zero results with `size:0`. ([20427](https://github.com/opensearch-project/OpenSearch/pull/20427))
- Fix the node local term and version being truncated in logs when host providers return very long IP or host strings ([20432](https://github.com/opensearch-project/OpenSearch/pull/20432))
- Remove child level directory on refresh for CompositeIndexWriter ([#20326](https://github.com/opensearch-project/OpenSearch/pull/20326))
- Fixes and refactoring in stream transport to make it more robust ([#20359](https://github.com/opensearch-project/OpenSearch/pull/20359))

### Dependencies
-

### Changed
- Migrate issue templates to 5.0.0 [(#855)](https://github.com/wazuh/wazuh-indexer/pull/855)
- Migrate workflows and scripts from 6.0.0 [(861)](https://github.com/wazuh/wazuh-indexer/pull/861)
- Migrate smoke tests to 5.0.0 [(#863)](https://github.com/wazuh/wazuh-indexer/pull/863)
- Replace and remove deprecated settings [(#894)](https://github.com/wazuh/wazuh-indexer/pull/894)
- Backport packaging improvements [(#906)](https://github.com/wazuh/wazuh-indexer/pull/906)
- Apply Lintian overrides [(#908)](https://github.com/wazuh/wazuh-indexer/pull/908)
- Add noninteractive option for DEB packages testing [(#914)](https://github.com/wazuh/wazuh-indexer/pull/914)
- Migrate smoke tests from Allocator to docker [(#931)](https://github.com/wazuh/wazuh-indexer/pull/931)
- Migrate builder workflows from [(#930)](https://github.com/wazuh/wazuh-indexer/pull/930)
- Rename bumper workflow file [(#986)](https://github.com/wazuh/wazuh-indexer/pull/986)
- Update previous version in debian workflow test [(#1041)](https://github.com/wazuh/wazuh-indexer/pull/1041)
- Disable multi-tenancy by default [(#1081)](https://github.com/wazuh/wazuh-indexer/pull/1081)
- Add version to the GH Workflow names [(#1124)](https://github.com/wazuh/wazuh-indexer/pull/1124)
- Update GitHub Actions versions in main branch [(#1131)](https://github.com/wazuh/wazuh-indexer/pull/1131)
- Refactor GH workflow to build packages to use a single branch input [(#1145)](https://github.com/wazuh/wazuh-indexer/pull/1145) [(#1169)](https://github.com/wazuh/wazuh-indexer/pull/1169)
- Enhance maintenance workflows [(#1192)](https://github.com/wazuh/wazuh-indexer/pull/1192)
- Change transport.port to http.port in indexer-security-init [(#1233)](https://github.com/wazuh/wazuh-indexer/pull/1233)
- Update builder script to detect SAP branch [(#1271)](https://github.com/wazuh/wazuh-indexer/pull/1271)
- Build SAP in CM workflow [(#1272)](https://github.com/wazuh/wazuh-indexer/pull/1272)
- Use docker commands directly instead of addnab/docker-run-action [(#1326)](https://github.com/wazuh/wazuh-indexer/pull/1326)

### Deprecated
-

### Removed
- Remove extra files [(#866)](https://github.com/wazuh/wazuh-indexer/pull/866) [(#1074)](https://github.com/wazuh/wazuh-indexer/pull/1074)
- Remove references to legacy VERSION file [(#908)](https://github.com/wazuh/wazuh-indexer/pull/908)
- Remove opensearch-performance-analyzer [(#892)](https://github.com/wazuh/wazuh-indexer/pull/892)

### Fixed
- Fix package upload to bucket subfolder 5.x [(#846)](https://github.com/wazuh/wazuh-indexer/pull/846)
- Fix seccomp error on `wazuh-indexer.service` [(#912)](https://github.com/wazuh/wazuh-indexer/pull/912)
- Fix CodeQL workflow [(#963)](https://github.com/wazuh/wazuh-indexer/pull/963)
- Fix auto-generated demo certificates naming [(#1010)](https://github.com/wazuh/wazuh-indexer/pull/1010)
- Fix service status preservation during upgrade in RPM packages [(#1031)](https://github.com/wazuh/wazuh-indexer/pull/1031)
- Fix Deprecation warning due to set-output command [(#1112)](https://github.com/wazuh/wazuh-indexer/pull/1112)
- Fix SysV service script permissions [(#1139)](https://github.com/wazuh/wazuh-indexer/pull/1139)
- Fix unscaped commands in indexer-security-init.sh [(#1196)](https://github.com/wazuh/wazuh-indexer/pull/1196)
- Fix broken link generation from the repository bumper script [(#1206)](https://github.com/wazuh/wazuh-indexer/pull/1206)
- Fix demo certificates generation triggered by default [(#1235)](https://github.com/wazuh/wazuh-indexer/pull/1235)

### Security
- Reduce risk of GITHUB_TOKEN exposure [(#960)](https://github.com/wazuh/wazuh-indexer/pull/960)
- Use latest Amazon Linux 2023 Docker image [(#1182)](https://github.com/wazuh/wazuh-indexer/pull/1182)
- Update CodeQL configuration [(#1220)](https://github.com/wazuh/wazuh-indexer/pull/1220)
- Potential fix for code scanning alerts: Workflow does not contain permissions [(#1234)](https://github.com/wazuh/wazuh-indexer/pull/1234)

[Unreleased 5.0.0]: https://github.com/wazuh/wazuh-indexer/compare/4.14.2...5.0.0
