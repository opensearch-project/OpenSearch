# Getting started with the composite data format

> **Status: experimental.** The composite engine and every plugin referenced here live
> under `sandbox/` and are annotated `@ExperimentalApi`. They carry **no**
> backwards-compatibility or support guarantees and may change or be removed at any time.

The **pluggable data format** removes the assumption that every OpenSearch index is stored
in Lucene. A shard can instead be backed by alternative storage engines, contributed by
`DataFormatPlugin` implementations that are discovered at node startup through the
`ExtensiblePlugin` SPI.

The **composite** format is the engine that coordinates them: it fans every document out to

- a **primary** format — the authoritative store that owns merges and commit coordination.
  The code default is `parquet`, a columnar format suited to analytical scans, and
- zero or more **secondary** formats that receive the same writes (commonly `lucene`, so
  full-text and term queries keep working).

The write path lives in `composite-engine`; the read path (serving analytical queries over
the columnar data) lives in the `analytics-engine` hub and its DataFusion backend. This
guide covers standing up the storage layer, creating and ingesting into a composite-backed
index, and running the associated tests. See the
[composite engine README](README.md) for the internal design.

## Prerequisites

| Requirement | Notes |
|---|---|
| **JDK 25** | The data-format plugins (`composite-engine`, `parquet-data-format`, `analytics-backend-datafusion`) and the analytics QA module compile and test against **JDK 25**, which is above the repo-wide minimum. Point `JAVA_HOME` at a JDK 25 when building or testing them. |
| **Rust + Cargo** | The engines ship a native Rust component under `sandbox/libs/dataformat-native`. Install a stable toolchain, e.g. via [rustup](https://rustup.rs/). |
| **protoc** | Required by the OpenSearch build; see the [Developer Guide](../../../DEVELOPER_GUIDE.md#install-prerequisites). |

## Run OpenSearch from source with the composite stack

From the repository root:

```bash
./gradlew run -PinstalledPlugins='["arrow-base","arrow-flight-rpc","composite-engine","parquet-data-format","analytics-backend-lucene"]'
```

| Plugin | Role |
|---|---|
| `arrow-base` | Apache Arrow runtime. **Must be listed first** — plugins that extend it fail the install jarHell check otherwise. |
| `arrow-flight-rpc` | Arrow Flight transport used by the native path. |
| `composite-engine` | Orchestrates the primary and secondary formats. |
| `parquet-data-format` | The default primary format; ships the native Rust component. |
| `analytics-backend-lucene` | Provides the `lucene` secondary format. |

Two things happen automatically when `parquet-data-format` or `analytics-backend-datafusion`
is in the list (see `gradle/run.gradle`): the experimental feature flag
`opensearch.experimental.feature.pluggable.dataformat.enabled` is turned on and the native
library path is configured. Local sandbox plugins are resolved from
`:sandbox:plugins:<name>`; plugins not found locally are fetched from Maven.

Wait for `started` in the console. This node can create composite indices, ingest, and merge.
For analytical **queries**, also see [Query the columnar data](#query-the-columnar-data-ppl).

## Create a composite-backed index

The data-format settings are **final** — they can only be set at index creation:

```bash
curl -X PUT "http://localhost:9200/logs-parquet" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": ["lucene"]
  },
  "mappings": {
    "properties": {
      "message":    { "type": "text" },
      "level":      { "type": "keyword" },
      "@timestamp": { "type": "date" }
    }
  }
}'
```

If you omit the `index.composite.*` settings, the cluster-level defaults are stamped in at
creation time (see the [settings reference](#settings-reference)).

## Ingest and verify

```bash
curl -X POST "http://localhost:9200/logs-parquet/_doc?refresh=true" -H 'Content-Type: application/json' -d '{
  "message": "connection reset by peer",
  "level": "ERROR",
  "@timestamp": "2026-01-01T00:00:00Z"
}'
```

Confirm the shard is healthy and the settings took effect:

```bash
curl "http://localhost:9200/_cat/indices/logs-parquet?v"
curl "http://localhost:9200/logs-parquet/_settings?pretty"
curl "http://localhost:9200/_plugins/composite/logs-parquet/_stats"
```

> **`_count` and `_search` return 0.** These APIs read through the **Lucene secondary**
> index, which does not materialize the Parquet-primary rows, so on a `parquet`-primary
> index they report **0 documents even after a successful ingest**. This is expected
> behavior, not data loss — the rows are there; read them through the analytics/PPL path
> below.

## Query the columnar data (PPL)

Analytical queries are served by the analytics engine and its DataFusion backend, fronted by
the PPL/SQL interface. Building the full read path means moving beyond the storage plugins:

- **Plugins** (install order matters; `arrow-base` first, `composite-engine` before the
  formats that extend it): `analytics-engine`, `analytics-backend-datafusion`,
  `dsl-query-executor`, plus the `opensearch-sql` and `opensearch-job-scheduler` plugins
  from Maven (they are not part of the sandbox source tree).
- **Feature flag:** `opensearch.experimental.feature.transport.stream.enabled=true` —
  fragment dispatch is streaming-only.
- **Recommended cluster setting:** `cluster.pluggable.dataformat: composite` — routes every
  PPL/SQL query to the analytics-engine path, including aliases and wildcard/resolved
  index expressions that the default per-index lookup does not handle.

The single authoritative, always-current reference for the exact plugin order, JVM
arguments, and settings this stack needs is the QA build definition at
[`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle) —
mirroring it is the surest way to get the read path working from source. Component detail:
[`analytics-engine`](../analytics-engine/README.md) and
[`analytics-backend-datafusion`](../analytics-backend-datafusion/README.md) READMEs.

With the stack running:

```bash
curl -X POST "http://localhost:9200/_plugins/_ppl" -H 'Content-Type: application/json' -d '{
  "query": "source = logs-parquet | stats count() by level"
}'
```

## Running the tests

Sandbox plugins use the same Gradle test tiers as the rest of OpenSearch. Test tasks are
per-project and are **not** gated by the `sandbox.enabled` property — that flag only decides
whether sandbox artifacts are bundled into a distribution
(`./gradlew assemble -Dsandbox.enabled=true`).

> **Before you run:** `JAVA_HOME` at a **JDK 25**, and **Rust/Cargo** installed — the
> data-format test tasks depend on `:sandbox:libs:dataformat-native:buildRustLibrary`.

### Unit tests (`src/test`)

```bash
# All unit tests for the plugin
./gradlew :sandbox:plugins:composite-engine:test

# Single class or method
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests"

# Reproduce with a seed, or repeat to surface flakiness
./gradlew :sandbox:plugins:composite-engine:test -Dtests.seed=DEADBEEF
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests" -Dtests.iters=50
```

### Internal cluster tests (`src/internalClusterTest`)

In-memory multi-node tests, enabled by the `opensearch.internal-cluster-test` plugin. The
composite-engine ones additionally need the Netty unsafe flags, the native library build, and
`arrow-base`/`analytics-engine` on the test classpath — all wired in the plugin's
`build.gradle`:

```bash
./gradlew :sandbox:plugins:composite-engine:internalClusterTest
```

### REST / QA integration tests

The end-to-end analytics stack is exercised by the QA module, which stands up a real
multi-node cluster with the full plugin set and JVM configuration:

```bash
# Full analytics REST suite (default two-node cluster)
./gradlew :sandbox:qa:analytics-engine-rest:integTest
```

Purpose-built variants exist for specific behaviors, each configuring the cluster
differently — plan-shape goldens (`integTestPlanShape`), memtable reduce
(`integTestMemetable`), query cache (`integTestQueryCache`), Arrow Flight streaming
(`integTestStreaming`), disabled segment merge (`integTestNoMerge`, sets
`opensearch.pluggable.dataformat.merge.enabled=false`), spill stats and spill cleanup
(`integTestSpillEnabled`, `integTestSpillCleanup`), and node-setting oversampling
(`integTestYmlOversampling`). Several are wired into `check`. See
[`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle)
for every variant.

To run the REST tests against an already-running node instead:

```bash
./gradlew :sandbox:qa:analytics-engine-rest:restTest -PrestCluster=localhost:9200
```

### Rust tests

The DataFusion native crate has Rust unit and fuzz tests wired into Gradle `check` via a
`cargoTest` task, and its fuzz seed follows the build-wide `-Dtests.seed`, so one failing
Java seed reproduces the Rust failure:

```bash
# Through Gradle (runs cargo test -p opensearch-datafusion --lib)
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest -PindexedE2eSeed=<hex>

# Or directly in the crate
cd sandbox/libs/dataformat-native/rust && cargo test -p opensearch-datafusion --lib
```

### Verification and formatting

```bash
# Unit tests + static analysis (Spotless, forbidden APIs, ...);
# for analytics-backend-datafusion this also runs cargoTest
./gradlew :sandbox:plugins:composite-engine:check

# Format checks only, and auto-fix formatting
./gradlew :sandbox:plugins:composite-engine:precommit
./gradlew spotlessApply
```

`check` does **not** include `internalClusterTest` — run that task explicitly; it is
comparatively heavy.

## Settings reference

### Index settings

| Setting | Default | Scope | Description |
|---|---|---|---|
| `index.pluggable.dataformat.enabled` | `false` | final | Master switch enabling the pluggable data format for the index (registered by core). |
| `index.pluggable.dataformat` | `""` | final | Data format to use; set to `composite` for primary + secondary (registered by core). |
| `index.composite.primary_data_format` | `parquet` | final | Authoritative format that owns merges and commit coordination. |
| `index.composite.secondary_data_formats` | `[]` | final | Formats written alongside the primary (registered by the composite engine). |
| `index.composite.merge_on_refresh_max_size` | `10mb` | dynamic | Total writer-segment size merged inline during refresh; above this, segments are committed individually for background merge. `0` disables merge-on-refresh. |

### Cluster settings

| Setting | Default | Scope | Description |
|---|---|---|---|
| `cluster.pluggable.dataformat.enabled` | `false` | dynamic | Cluster-wide default for the index master switch (registered by core). |
| `cluster.pluggable.dataformat` | `""` | dynamic | Cluster-wide data format; set to `composite` to route PPL/SQL queries to the analytics path (registered by core). |
| `cluster.composite.primary_data_format` | `parquet` | dynamic | Default primary format stamped into new composite indices. |
| `cluster.composite.secondary_data_formats` | `[]` | dynamic | Default secondary formats stamped into new composite indices. |
| `cluster.restrict.composite.dataformat` | `false` | dynamic | When `true`, index-creation requests whose `index.composite.*` settings differ from the cluster defaults are rejected; when `false`, the format is chosen per-index. |

Two behaviors worth knowing: the `index.composite.*` settings are **final**, so later updates
to the `cluster.composite.*` defaults affect only indices created afterward; and rejected
requests raise an index-creation validation error naming the setting that differed.

### Feature flags (node JVM system properties)

| Property | Required for |
|---|---|
| `opensearch.experimental.feature.pluggable.dataformat.enabled` | Any composite / pluggable data format index. |
| `opensearch.experimental.feature.transport.stream.enabled` | The analytics query path (streaming fragment dispatch). |

## Limitations and troubleshooting

- **Nested objects** are not supported by the composite format — a mapping with nested
  fields fails at index creation with a "not supported by the [composite] data format" error.
- **`UnsatisfiedLinkError` / missing native library** — the native library is not built or
  not on `java.library.path`. `./gradlew run` and the test tasks build it automatically; a
  manually started node must build it
  (`./gradlew :sandbox:libs:dataformat-native:buildRustLibrary`) and set
  `-Djava.library.path=<repo>/sandbox/libs/dataformat-native/rust/target/release`.
- **`IllegalArgumentException` when updating `index.pluggable.dataformat*` /
  `index.composite.primary_data_format`** — these settings are **final**; recreate the index.
- **`_count` / `_search` report 0 documents** — expected on a `parquet`-primary index (see
  [Ingest and verify](#ingest-and-verify)); query via the analytics/PPL path.
- **Compilation errors referencing FFM or newer APIs** — the data-format plugins require
  **JDK 25**; verify `JAVA_HOME`.
- **PPL query fails or the analytics endpoint is unavailable** — the query stack needs the
  analytics plugins **and** `opensearch.experimental.feature.transport.stream.enabled=true`
  (plus the recommended `cluster.pluggable.dataformat` setting). Compare your node against
  [`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle).

## Further reading

- [Developer Guide](../../../DEVELOPER_GUIDE.md) — building, testing, and the `sandbox` layout.
- [`composite-engine/README.md`](README.md) — composite engine architecture and key classes.
- [`analytics-engine/README.md`](../analytics-engine/README.md) — query hub and SPI wiring.
- [`analytics-backend-datafusion/README.md`](../analytics-backend-datafusion/README.md) — native execution backend.
- [`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle) — authoritative end-to-end cluster configuration.
- [TESTING.md](../../../TESTING.md) — repo-wide test conventions.
