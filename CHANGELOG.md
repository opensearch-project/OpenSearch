# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.1.x]
### Added

### Changed

### Dependencies

### Deprecated

### Removed

### Fixed
- Add task cancellation checks in aggregators ([#18426](https://github.com/opensearch-project/OpenSearch/pull/18426))
- Fix concurrent timings in profiler ([#18540](https://github.com/opensearch-project/OpenSearch/pull/18540))
- Fix regex query from query string query to work with field alias ([#18215](https://github.com/opensearch-project/OpenSearch/issues/18215))
- [Autotagging] Fix delete rule event consumption in InMemoryRuleProcessingService ([#18628](https://github.com/opensearch-project/OpenSearch/pull/18628))
- Cannot communicate with HTTP/2 when reactor-netty is enabled ([#18599](https://github.com/opensearch-project/OpenSearch/pull/18599))
- Fix the visit of sub queries for HasParentQuery and HasChildQuery ([#18621](https://github.com/opensearch-project/OpenSearch/pull/18621))
- Fix the backward compatibility regression with COMPLEMENT for Regexp queries introduced in OpenSearch 3.0 ([#18640](https://github.com/opensearch-project/OpenSearch/pull/18640))

### Security

