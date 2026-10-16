## Version 2.19.7 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.7

### Enhancements

* Add upper bound for the hot_threads `snapshots` parameter with configurable `monitor.jvm.hot_threads.max_snapshots` cluster setting ([#23144](https://github.com/opensearch-project/OpenSearch/pull/23144))
* Add `search.query.max_query_nesting_depth` setting to limit query_string parenthesis nesting depth and prevent StackOverflowError ([#22854](https://github.com/opensearch-project/OpenSearch/pull/22854))
* Cap `max_determinized_states` in completion suggester regex and fuzzy options to prevent heap exhaustion ([#22925](https://github.com/opensearch-project/OpenSearch/pull/22925))
* Disable Mustache partial template resolution in search templates ([#22440](https://github.com/opensearch-project/OpenSearch/pull/22440))
* Reject out-of-range WLM node threshold updates at validation time ([#22678](https://github.com/opensearch-project/OpenSearch/pull/22678))

### Bug Fixes

* Fix unbounded recursion in JSON and binary deserialization that could cause StackOverflowError ([#22427](https://github.com/opensearch-project/OpenSearch/pull/22427))
* Strengthen scroll_id validation to prevent OOM from crafted scroll IDs with oversized counts ([#22401](https://github.com/opensearch-project/OpenSearch/pull/22401))
* Validate `base_path` in fs repository to prevent path traversal outside configured `path.repo` directories ([#22381](https://github.com/opensearch-project/OpenSearch/pull/22381))
* Fix handling of unsupported zstd HTTP content encoding in netty4 and nio transports ([#23324](https://github.com/opensearch-project/OpenSearch/pull/23324))

### Maintenance

* Bump Jackson to 2.18.9 and Log4j to 2.25.5 ([#22990](https://github.com/opensearch-project/OpenSearch/pull/22990))
* Bump Bouncy Castle bc-fips to 2.1.3 ([#23284](https://github.com/opensearch-project/OpenSearch/pull/23284))
* Update Netty to 4.1.139.Final ([#22692](https://github.com/opensearch-project/OpenSearch/pull/22692), [#23003](https://github.com/opensearch-project/OpenSearch/pull/23003), [#23251](https://github.com/opensearch-project/OpenSearch/pull/23251))
* Update bundled JDK to 21.0.12.1+1 ([#22572](https://github.com/opensearch-project/OpenSearch/pull/22572), [#22809](https://github.com/opensearch-project/OpenSearch/pull/22809))
