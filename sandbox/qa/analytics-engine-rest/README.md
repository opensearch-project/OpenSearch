# Analytics Engine REST Integration Tests

REST-based integration tests for the analytics engine, running against a live OpenSearch cluster with sandbox plugins installed.

## Architecture

```
AnalyticsRestTestCase           ← abstract base (cluster config, helpers)
├── ParquetDataFusionIT         ← parquet indexing sanity + index settings validation
├── DslClickBenchIT             ← DSL queries via _search → DataFusion
└── PplClickBenchIT             ← PPL queries via /_analytics/ppl → DataFusion

Dataset                         ← descriptor for a test dataset (mapping, bulk data, queries)
DatasetProvisioner              ← provisions any dataset into a parquet-backed index
DatasetQueryRunner              ← auto-discovers queries and runs them against a cluster
ClickBenchTestHelper            ← ClickBench dataset constants
```

- `AnalyticsRestTestCase` — handles cluster preservation, resource loading, JSON escaping, and assertion helpers. Extend this for any new integration test.
- `Dataset` / `DatasetProvisioner` / `DatasetQueryRunner` — generic test infrastructure. Any new dataset can plug in by adding a directory under `resources/datasets/{name}/`.
- `ClickBenchTestHelper` — thin wrapper that declares the ClickBench dataset descriptor.

## Adding a New Dataset

To add a new dataset, create a directory under `src/test/resources/datasets/{name}/` with this structure:

```
datasets/
  {name}/
    mapping.json              # index mapping + settings
    bulk.json                 # bulk-indexable documents (NDJSON)
    dsl/q1.json ... qN.json   # DSL queries (auto-discovered)
    dsl/expected/q1.json ...  # expected responses (optional)
    ppl/q1.ppl ... qN.ppl     # PPL queries (auto-discovered)
    ppl/expected/q1.json ...  # expected responses (optional)
```

Then declare the dataset in Java:

```java
Dataset myDataset = new Dataset("myDatasetName", "my_index_name");
```

`DatasetProvisioner.provision(client, myDataset)` creates the index with parquet data format and ingests the bulk data. `DatasetQueryRunner.discoverQueryNumbers(myDataset, "dsl")` auto-discovers all query files.

## Test Classes

| Test | Description |
|------|-------------|
| `ParquetDataFusionIT` | Sanity check: creates a parquet-format index, validates settings are persisted, ingests docs, runs a simple search |
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

## Running Tests

### Managed testClusters (integTest) — auto-provisioned

The `integTest` task auto-starts a single-node cluster with all required plugins and runs the tests:

```bash
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true
```

The cluster configuration (plugins, feature flag, native library path) is defined in `build.gradle` — no manual setup needed.

### External cluster (restTest) — manually provisioned

Start a cluster manually (see below), then run tests against it:

```bash
# Default: localhost:9200
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true

# Custom cluster
./gradlew :sandbox:qa:analytics-engine-rest:restTest -Dsandbox.enabled=true -PrestCluster=host:port
```

### Starting a cluster manually

```bash
./gradlew publishToMavenLocal -Dsandbox.enabled=true -x test -x javadoc

NATIVE_LIB_DIR=$(pwd)/sandbox/libs/dataformat-native/rust/target/release

./gradlew run -Dsandbox.enabled=true \
  -PinstalledPlugins="['analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', 'analytics-backend-lucene', 'dsl-query-executor', 'composite-engine', 'test-ppl-frontend']" \
  -Dtests.jvm.argline="-Djava.library.path=$NATIVE_LIB_DIR -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true" \
  -x javadoc -x test -x missingJavadoc
```

Note: PPL tests via `/_analytics/ppl` require the `test-ppl-frontend` plugin. It is included in the `integTest` cluster config and can also be added to `./gradlew run` via `-PinstalledPlugins`.

### Running individual tests

```bash
# Parquet sanity
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true \
  --tests "org.opensearch.analytics.qa.ParquetDataFusionIT"

# DSL ClickBench
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true \
  --tests "org.opensearch.analytics.qa.DslClickBenchIT"

# PPL ClickBench
./gradlew :sandbox:qa:analytics-engine-rest:integTest -Dsandbox.enabled=true \
  --tests "org.opensearch.analytics.qa.PplClickBenchIT"
```

## Notes

- Parquet indexing uses the composite data format framework: `index.composite.primary_data_format = parquet`
- The `pluggable.dataformat.enabled` feature flag must be set at cluster startup (already configured for `integTest`)
- DSL path: `_search` → dsl-query-executor → Calcite planning → Substrait → DataFusion
- PPL path: `/_analytics/ppl` → test-ppl-frontend → analytics-engine → Calcite → Substrait → DataFusion
- Expected response validation (via `{language}/expected/q{N}.json`) is planned for future iterations — currently the runner only validates that responses are non-empty
- `DslClickBenchIT` runs ClickBench Q1. Auto-discovery of all 43 DSL queries is commented out in the test (see class javadoc) because several queries exercise unsupported translators/planner rules and destabilize the shared cluster. Re-enable as support expands.
- `PplClickBenchIT` runs ClickBench Q1 via the test-ppl-frontend plugin. Auto-discovery is commented out for the same reason as DSL.
