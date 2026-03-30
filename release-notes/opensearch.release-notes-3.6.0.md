## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Features

* Add bitmap64 query support ([#20606](https://github.com/opensearch-project/OpenSearch/pull/20606))
* Add warmup phase for pull-based ingestion ([#20526](https://github.com/opensearch-project/OpenSearch/pull/20526))
* WLM group custom search settings ([#20536](https://github.com/opensearch-project/OpenSearch/pull/20536))

### Enhancements

* Add mapper_settings support and field_mapping mapper type for pull-based ingestion ([#20722](https://github.com/opensearch-project/OpenSearch/pull/20722))
* Add node-level JVM and CPU runtime metrics ([#20844](https://github.com/opensearch-project/OpenSearch/pull/20844))
* Adding stream request flag to SearchRequestContext ([#20530](https://github.com/opensearch-project/OpenSearch/pull/20530))
* Choose the best performing node when writing with append only index ([#20065](https://github.com/opensearch-project/OpenSearch/pull/20065))
* Fallback to netty client if AWS Crt client is not available on the target platform / architecture ([#20698](https://github.com/opensearch-project/OpenSearch/pull/20698))
* Fix ProfileScorer.getChildren() to expose wrapped scorer in scorer tree ([#20607](https://github.com/opensearch-project/OpenSearch/pull/20607))
* Fix terms lookup subquery to use cluster max_clause_count setting ([#20823](https://github.com/opensearch-project/OpenSearch/pull/20823))
* Implement FieldMappingIngestionMessageMapper for pull-based ingestion ([#20729](https://github.com/opensearch-project/OpenSearch/pull/20729))
* Intra segment support for single-value metric aggregations ([#20503](https://github.com/opensearch-project/OpenSearch/pull/20503))
* Support Docker distribution builds for ppc64le, arm64 and s390x ([#20678](https://github.com/opensearch-project/OpenSearch/pull/20678))

### Bug Fixes

* Add static method to expose STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED index option ([#20980](https://github.com/opensearch-project/OpenSearch/pull/20980))
* Disable concurrent search for filter duplicates in significant_text ([#20857](https://github.com/opensearch-project/OpenSearch/pull/20857))
* Fix JSON escaping in task details log metadata ([#20802](https://github.com/opensearch-project/OpenSearch/pull/20802))
* Fix array_index_out_of_bounds_exception with wildcard and aggregations ([#20842](https://github.com/opensearch-project/OpenSearch/pull/20842))
* Fix copy_to functionality for geo_point fields with object/array values ([#20542](https://github.com/opensearch-project/OpenSearch/pull/20542))
* Fix field_caps returning empty results for disable_objects mappings ([#20814](https://github.com/opensearch-project/OpenSearch/pull/20814))
* Fix flaky OpenSearchTestBasePluginFuncTest ([#20975](https://github.com/opensearch-project/OpenSearch/pull/20975))
* Fix the regression of terms agg optimization ([#20623](https://github.com/opensearch-project/OpenSearch/pull/20623))
* Fix/collision of indexpatterns ([#20702](https://github.com/opensearch-project/OpenSearch/pull/20702))
* Handle dependencies between analyzers ([#19248](https://github.com/opensearch-project/OpenSearch/pull/19248))
* Harden detection of HTTP/3 support by ensuring Quic native libraries are available for the target platform ([#20680](https://github.com/opensearch-project/OpenSearch/pull/20680))
* Implementing batched deletions of stale ClusterMetadataManifests in RemoteClusterStateCleanupManager and adjusting the timeouts ([#20566](https://github.com/opensearch-project/OpenSearch/pull/20566))
* Improve exception messaging when encountering legacy version IDs ([#20512](https://github.com/opensearch-project/OpenSearch/pull/20512))
* Lazy init stored field reader in SourceLookup ([#20827](https://github.com/opensearch-project/OpenSearch/pull/20827))
* Leveraging segment-global ordinal mapping for efficient terms aggregation ([#20683](https://github.com/opensearch-project/OpenSearch/pull/20683))
* Make stale segments cleanup logic depend on map size as well ([#20976](https://github.com/opensearch-project/OpenSearch/pull/20976))
* Service does not start on Windows with OpenJDK (update to procrun 1.5.1) ([#20615](https://github.com/opensearch-project/OpenSearch/pull/20615))
* Show heap percent threshold in cancellation message ([#20779](https://github.com/opensearch-project/OpenSearch/pull/20779))
* [GRPC] Add accessUnixDomainSocket permission for transport-grpc ([#20649](https://github.com/opensearch-project/OpenSearch/pull/20649))

### Infrastructure

* Bump Apache Lucene from 10.3.2 to 10.4.0 ([#20735](https://github.com/opensearch-project/OpenSearch/pull/20735))
* Bump OpenTelemetry to 1.59.0 and OpenTelemetry Semconv to 1.40.0 ([#20737](https://github.com/opensearch-project/OpenSearch/pull/20737))
* Bump OpenTelemetry to 1.60.1 ([#20797](https://github.com/opensearch-project/OpenSearch/pull/20797))
* Bump Project Reactor to 3.8.4 and Reactor Netty to 1.3.4 ([#20834](https://github.com/opensearch-project/OpenSearch/pull/20834))
* Bump actions/upload-artifact from 6 to 7 ([#20762](https://github.com/opensearch-project/OpenSearch/pull/20762))
* Bump ch.qos.logback:logback-classic from 1.5.27 to 1.5.32 in /test/fixtures/hdfs-fixture ([#20761](https://github.com/opensearch-project/OpenSearch/pull/20761))
* Bump com.netflix.nebula.ospackage-base from 12.2.0 to 12.3.0 in /distribution/packages ([#20799](https://github.com/opensearch-project/OpenSearch/pull/20799))
* Bump com.nimbusds:nimbus-jose-jwt from 10.7 to 10.8 in /test/fixtures/hdfs-fixture ([#20715](https://github.com/opensearch-project/OpenSearch/pull/20715))
* Bump com.sun.xml.bind:jaxb-impl from 4.0.6 to 4.0.7 in /plugins/discovery-azure-classic ([#20886](https://github.com/opensearch-project/OpenSearch/pull/20886))
* Bump lycheeverse/lychee-action from 2.7.0 to 2.8.0 ([#20888](https://github.com/opensearch-project/OpenSearch/pull/20888))
* Bump org.jline:jline from 3.30.6 to 4.0.0 in /test/fixtures/hdfs-fixture ([#20800](https://github.com/opensearch-project/OpenSearch/pull/20800))
* Bump org.jruby.jcodings:jcodings from 1.0.63 to 1.0.64 in /libs/grok ([#20713](https://github.com/opensearch-project/OpenSearch/pull/20713))
* Bump org.jruby.joni:joni from 2.2.3 to 2.2.6 ([#20714](https://github.com/opensearch-project/OpenSearch/pull/20714))
* Bump org.jruby.joni:joni from 2.2.6 to 2.2.7 in /libs/grok ([#20759](https://github.com/opensearch-project/OpenSearch/pull/20759))
* Bump org.tukaani:xz from 1.11 to 1.12 in /plugins/ingest-attachment ([#20760](https://github.com/opensearch-project/OpenSearch/pull/20760))
* Bump shadow-gradle-plugin from 8.3.9 to 9.3.1 ([#20569](https://github.com/opensearch-project/OpenSearch/pull/20569))
* Bump tj-actions/changed-files from 47.0.1 to 47.0.2 ([#20638](https://github.com/opensearch-project/OpenSearch/pull/20638))
* Bump tj-actions/changed-files from 47.0.2 to 47.0.4 ([#20716](https://github.com/opensearch-project/OpenSearch/pull/20716))
* Bump tj-actions/changed-files from 47.0.4 to 47.0.5 ([#20801](https://github.com/opensearch-project/OpenSearch/pull/20801))
* Update Jackson to 2.21.2 ([#20989](https://github.com/opensearch-project/OpenSearch/pull/20989))
* Update Netty to 4.2.11.Final ([#20997](https://github.com/opensearch-project/OpenSearch/pull/20997))
* Update Netty to 4.2.12.Final ([#20998](https://github.com/opensearch-project/OpenSearch/pull/20998))

### Maintenance

* Bump `com.netflix.nebula:gradle-info-plugin` to 16.2.1 ([#20825](https://github.com/opensearch-project/OpenSearch/pull/20825))
* Ensure that transient ThreadContext headers with propagators survive restore ([#20854](https://github.com/opensearch-project/OpenSearch/pull/20854))
* Remove incorrect entries re-added by 3154811 ([#20853](https://github.com/opensearch-project/OpenSearch/pull/20853))
* Revert "Disable concurrent search for filter duplicates" ([#20915](https://github.com/opensearch-project/OpenSearch/pull/20915))
* [AUTO] [main] Add bwc version 2.19.6. ([#20274](https://github.com/opensearch-project/OpenSearch/pull/20274))
* [Pull-based Ingestion] Remove experimental tag for pull-based ingestion ([#20704](https://github.com/opensearch-project/OpenSearch/pull/20704))

### Refactoring

* Add indexer interface for shard interaction with underlying engines ([#20675](https://github.com/opensearch-project/OpenSearch/pull/20675))
* Add new setting property 'Sensitive' for tiering dynamic settings ([#20901](https://github.com/opensearch-project/OpenSearch/pull/20901))
* Delegate getMin/getMax methods for ExitableTerms ([#20775](https://github.com/opensearch-project/OpenSearch/pull/20775))
* Flight transport TLS cert hot-reload ([#20700](https://github.com/opensearch-project/OpenSearch/pull/20700))
* Fix stream transport TLS cert hot-reload by using live SSLContext from SecureTransportSettingsProvider ([#20734](https://github.com/opensearch-project/OpenSearch/pull/20734))
* Introducing indexing & deletion strategy planner interfaces ([#20585](https://github.com/opensearch-project/OpenSearch/pull/20585))
* Make Telemetry Tags Immutable ([#20788](https://github.com/opensearch-project/OpenSearch/pull/20788))
* [GRPC] Handle ShardSearchFailure properly ([#20641](https://github.com/opensearch-project/OpenSearch/pull/20641))

### Documentation

* Feat(hunspell): Add ref_path support for package-based dictionary loading ([#20840](https://github.com/opensearch-project/OpenSearch/pull/20840))

### Unknown

* Add support of IndexWarmer for replica shards with segment replication enabled ([#20650](https://github.com/opensearch-project/OpenSearch/pull/20650))