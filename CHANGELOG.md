As of the 3.6 release [the CHANGELOG is no longer used][1] to generate release notes.

[Use this PR search][2] to browse unreleased changes.

[1]: https://github.com/opensearch-project/OpenSearch/issues/21071
[2]: https://github.com/opensearch-project/OpenSearch/pulls?q=sort%3Amerged-desc+is%3Apr+-label%3Askip-changelog+is%3Amerged+base%3Amain+

## [Unreleased 3.x]
### Added
- Add `cluster.routing.allocation.balance.disk_usage` setting for disk-usage-aware shard rebalancing ([#15520](https://github.com/opensearch-project/OpenSearch/issues/15520))
