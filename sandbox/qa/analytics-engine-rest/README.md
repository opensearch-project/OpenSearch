# Analytics Engine REST Integration Tests

REST-based integration tests for the analytics engine's DataFusion and Parquet query paths, running against a live OpenSearch cluster with sandbox plugins installed.

## Architecture

```
DataFusionRestTestCase          ← abstract base (cluster config, helpers)
├── ParquetDataFusionIT         ← pure parquet indexing + query end-to-end
├── DslClickBenchIT             ← DSL queries via _search → DataFusion
└── PplClickBenchIT             ← PPL queries via /_analytics/ppl → DataFusion

ClickBenchTestFixture           ← static helper (ClickBench parquet index provisioning, query loading)
```

- `DataFusionRestTestCase` — handles cluster preservation, resource loading, JSON escaping, and assertion helpers. Extend this for any new integration test.
- `ClickBenchTestFixture` — stateless utility that provisions the ClickBench 100-row dataset. Used via composition, not inheritance.

## Test Classes

| Test | Description |
|------|-------------|
| `ParquetDataFusionIT` | Creates a parquet-format index, bulk-indexes documents, flushes, and queries via `_search` |
| `DslClickBenchIT` | Runs ClickBench DSL queries via `_search` → dsl-query-executor → Calcite → Substrait → DataFusion |
| `PplClickBenchIT` | Runs ClickBench PPL queries via `/_analytics/ppl` → test-ppl-frontend → analytics-engine → Calcite → Substrait → DataFusion |

## Prerequisites

### JDK 25+

The sandbox requires JDK 25 or newer:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home  # macOS example
```

### Rust toolchain (native library)

The DataFusion backend requires a native Rust library. Build it once (re-run after Rust code changes):

```bash
./gradlew :sandbox:libs:dataformat-native:buildRustLibrary -Dsandbox.enabled=true
```

### Publish to local Maven

The `./gradlew run` command resolves plugins from Maven. Publish core + sandbox plugins:

```bash
./gradlew publishToMavenLocal -Dsandbox.enabled=true -x test -x javadoc
```

## Running Tests

### Via managed testClusters (integTest)

```bash
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true
```

### Via external cluster (restTest)

Start a cluster manually (see below), then run:

```bash
# Default: localhost:9200
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true

# Custom cluster
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true -PrestCluster=host:port
```

### Starting a cluster manually

The native library must be on `java.library.path` for the DataFusion plugin to load.
Note: `test-ppl-frontend` is a sandbox QA plugin and is not available via `./gradlew run -PinstalledPlugins`.
PPL tests should be run via `integTest` (managed testClusters) which uses project references.

```bash
NATIVE_LIB_DIR=$(pwd)/sandbox/libs/dataformat-native/rust/target/release

# Cluster for DSL + Parquet tests (no PPL)
./gradlew run -Dsandbox.enabled=true \
  -PinstalledPlugins="['analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', 'analytics-backend-lucene', 'dsl-query-executor', 'composite-engine']" \
  -Dtests.jvm.argline="-Djava.library.path=$NATIVE_LIB_DIR -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true" \
  -x javadoc -x test -x missingJavadoc
```

### Running individual tests (against a running cluster)

```bash
# Parquet end-to-end (indexing + query)
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true \
  --tests "org.opensearch.analytics.qa.ParquetDataFusionIT" \
  -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=runTask

# DSL ClickBench
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true \
  --tests "org.opensearch.analytics.qa.DslClickBenchIT" \
  -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=runTask

# PPL ClickBench (via test-ppl-frontend, run via integTest)
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true \
  --tests "org.opensearch.analytics.qa.PplClickBenchIT" \
  -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=runTask
```

## Test Resources

```
src/test/resources/clickbench/
  parquet_hits_mapping.json   # Index mapping (ClickBench schema, 103 fields)
  bulk.json                   # 100 documents for ClickBench tests
  dsl/q1.json ... q43.json   # 43 ClickBench DSL queries
  ppl/q1.ppl ... q43.ppl     # 43 ClickBench PPL queries
```

## Notes

- Parquet indexing uses the composite data format framework: `index.composite.primary_data_format = parquet`
- The `pluggable.dataformat.enabled` feature flag must be set as a JVM system property at cluster startup
- DSL path: `_search` → dsl-query-executor → Calcite planning → Substrait → DataFusion
- PPL path: `/_analytics/ppl` → test-ppl-frontend → analytics-engine → Calcite → Substrait → DataFusion
