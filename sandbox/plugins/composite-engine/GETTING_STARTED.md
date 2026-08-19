# Getting Started with the Pluggable Data Format

> **Experimental.** The pluggable data format and every plugin described here live in
> `sandbox/` and are marked `@ExperimentalApi`. They carry no backwards-compatibility or
> long-term-support guarantees and may change or be removed at any time.

This guide shows how to spin up OpenSearch from source with the **pluggable data format**
enabled, create an index backed by the **composite** format (Parquet primary + Lucene
secondary), ingest a document, and verify it.

## What the pluggable data format is

Classic OpenSearch stores every index in Lucene. The pluggable data format lets an index
be backed by one or more *data format* engines instead. The engine that ties them together
is the **composite** format, which writes each document to:

- a **primary** format — the authoritative store used for merges and commit coordination
  (default: `parquet`), and
- zero or more **secondary** formats that receive the same writes (commonly `lucene`, so
  text/keyword predicates still resolve).

Each format is contributed by a `DataFormatPlugin` (see the SPI in
`server/.../index/engine/dataformat/` and the architecture notes in
[`README.md`](README.md)). The composite plugin discovers the format plugins at node
startup via the `ExtensiblePlugin` SPI.

## Prerequisites

- The standard OpenSearch build prerequisites (JDK 21+, and **Rust + protoc** — the
  Parquet/DataFusion plugins compile a native Rust component). See the
  [Developer Guide](../../../DEVELOPER_GUIDE.md#install-prerequisites).
- `JAVA_HOME` set to your JDK.

## 1. Run OpenSearch with the data-format plugins

From the repository root, launch a single node with the composite stack installed. The
`run` task automatically enables the pluggable data format feature flag and sets the native
library path whenever `parquet-data-format` or `analytics-backend-datafusion` is in the
plugin list (see `gradle/run.gradle`):

```bash
./gradlew run -PinstalledPlugins='["arrow-base","arrow-flight-rpc","composite-engine","parquet-data-format","analytics-backend-lucene"]'
```

- `composite-engine` — orchestrates primary + secondary formats.
- `parquet-data-format` — the default primary format (native Rust component).
- `analytics-backend-lucene` — provides the `lucene` secondary format.
- `arrow-base` / `arrow-flight-rpc` — Arrow runtime the native path depends on. `arrow-base`
  must be listed **before** any plugin that extends it.

This corresponds to setting, on a manually-configured node:

```yaml
# opensearch.yml
opensearch.experimental.feature.pluggable.dataformat.enabled: true
```

Wait for `[node-1] started` in the console.

## 2. Create a composite-backed index

The two index settings that turn on the pluggable data format are **final** — they can only
be set at index-creation time:

```bash
curl -X PUT "http://localhost:9200/logs-parquet" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.composite.secondary_data_formats": ["lucene"]
  },
  "mappings": {
    "properties": {
      "message":   { "type": "text" },
      "level":     { "type": "keyword" },
      "@timestamp":{ "type": "date" }
    }
  }
}'
```

| Setting | Default | Notes |
|---|---|---|
| `index.pluggable.dataformat.enabled` | `false` | Final. Master switch for the index. |
| `index.pluggable.dataformat` | `""` | Final. Set to `composite` to use the composite engine. |
| `index.composite.primary_data_format` | `parquet` | Final. Authoritative / merge format. |
| `index.composite.secondary_data_formats` | `[]` | Final. Extra formats written alongside the primary. |

You can also set cluster-wide defaults so new indices pick up the format automatically
(`cluster.pluggable.dataformat.enabled`, `cluster.pluggable.dataformat`,
`cluster.composite.primary_data_format`, `cluster.composite.secondary_data_formats`).

## 3. Index a document

```bash
curl -X POST "http://localhost:9200/logs-parquet/_doc?refresh=true" -H 'Content-Type: application/json' -d '{
  "message": "connection reset by peer",
  "level": "ERROR",
  "@timestamp": "2026-01-01T00:00:00Z"
}'
```

## 4. Verify

> **Gotcha:** `_count` and `_search` hit the **Lucene secondary** index, which does not
> materialize the Parquet-primary rows — so on a parquet-primary index `_count` reports
> **0 docs even after a successful ingest**. That is expected, not data loss.

Confirm the shard is healthy and the format took effect:

```bash
# Index exists and is green/yellow
curl "http://localhost:9200/_cat/indices/logs-parquet?v"

# Settings reflect the composite format
curl "http://localhost:9200/logs-parquet/_settings?pretty"
```

To actually **query** the Parquet data you need the analytics query stack (DataFusion
backend + a PPL/SQL front end). Add those plugins when launching:

```bash
./gradlew run -PinstalledPlugins='["arrow-base","arrow-flight-rpc","composite-engine","parquet-data-format","analytics-backend-lucene","analytics-backend-datafusion","analytics-engine","dsl-query-executor"]'
```

The analytics engine additionally requires the streaming transport feature flag
(`opensearch.experimental.feature.transport.stream.enabled=true`). For a fully-wired,
ready-to-run example of the complete stack — plugin order, JVM flags, and cluster settings
— see the QA harness at
[`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle)
and the [`analytics-engine`](../analytics-engine/README.md) README.

## Running tests

Sandbox plugins use the same test tiers as the rest of OpenSearch. There is **no special
`sandbox.enabled` flag for tests** — that flag only controls whether sandbox artifacts are
bundled into a *distribution* (`./gradlew assemble -Dsandbox.enabled=true`). Test tasks run
per-project directly.

> **JDK note:** the data-format plugins (`composite-engine`, `parquet-data-format`,
> `analytics-backend-datafusion`) compile and test against **JDK 25**, not the repo-wide
> JDK 21 minimum. Point `JAVA_HOME` (or the Gradle runtime JDK) at a JDK 25 before running
> their tests. Any task that touches the native path also depends on
> `:sandbox:libs:dataformat-native:buildRustLibrary`, so **Rust/Cargo must be installed**.

### Unit tests (`src/test`, class names end in `Tests`)

```bash
# One plugin's unit tests
./gradlew :sandbox:plugins:composite-engine:test
./gradlew :sandbox:plugins:analytics-backend-datafusion:test

# A single test class or method
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests"
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests.testRollback"

# Reproduce a failure with its seed, or repeat to shake out flakiness
./gradlew :sandbox:plugins:composite-engine:test -Dtests.seed=DEADBEEF
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests.testRollback" -Dtests.iters=50
```

### Internal cluster tests (`src/internalClusterTest`, class names end in `IT`)

These spin up an in-memory multi-node cluster. Enabled by
`apply plugin: 'opensearch.internal-cluster-test'` in the plugin's `build.gradle`; the
data-format plugins add the Netty/native JVM flags and the Rust build dependency for you.

```bash
./gradlew :sandbox:plugins:composite-engine:internalClusterTest
./gradlew :sandbox:plugins:composite-engine:internalClusterTest --tests "*.CompositeParquetIndexIT"
```

Plugins with `internalClusterTest` sources today: `composite-engine`,
`analytics-engine`, `analytics-backend-datafusion`, `dsl-query-executor`,
`block-cache-foyer`, and the `native-repository-*` plugins.

### REST / QA integration tests (`sandbox/qa`)

The end-to-end analytics stack (composite + Parquet + DataFusion + PPL) is exercised from
the QA modules, which stand up a real test cluster with the full plugin set and JVM flags:

```bash
# Full analytics REST suite (default 2-node cluster)
./gradlew :sandbox:qa:analytics-engine-rest:integTest

# Named variants (each configures the cluster differently — see the build.gradle)
./gradlew :sandbox:qa:analytics-engine-rest:integTestNoMerge      # parquet merge disabled
./gradlew :sandbox:qa:analytics-engine-rest:integTestStreaming    # Arrow Flight streaming
./gradlew :sandbox:qa:analytics-engine-rest:integTestMemtable

# Coordinator-reduce internal cluster tests
./gradlew :sandbox:qa:analytics-engine-coordinator:internalClusterTest

# Run the REST tests against an already-running external cluster
./gradlew :sandbox:qa:analytics-engine-rest:restTest -PrestCluster=localhost:9200
```

`sandbox/qa/analytics-engine-rest/build.gradle` is the authoritative reference for the exact
plugin order, JVM flags, and cluster settings the stack needs.

### Rust tests

The DataFusion native crate has Rust unit + fuzz tests wired into Gradle `check` via a
`cargoTest` task:

```bash
# Via Gradle (runs `cargo test -p opensearch-datafusion --lib`)
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest

# The fuzz seed follows -Dtests.seed (or override explicitly)
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest -PindexedE2eSeed=<hex>

# Or run cargo directly in the crate
cd sandbox/libs/dataformat-native/rust && cargo test -p opensearch-datafusion --lib
```

### Everything for one plugin

```bash
# Unit tests + static analysis (spotless, forbidden APIs, etc.); for
# analytics-backend-datafusion this also runs cargoTest.
./gradlew :sandbox:plugins:composite-engine:check

# Static/format checks only
./gradlew :sandbox:plugins:composite-engine:precommit
./gradlew spotlessApply        # auto-fix formatting
```

Note: `check` does **not** run `internalClusterTest` — run that task explicitly (it is heavy).

## Where to go next

- [`composite-engine/README.md`](README.md) — architecture and key classes of the composite engine.
- `server/.../index/engine/dataformat/DataFormatPlugin.java` — the SPI to implement if you
  want to contribute a **new** data format.
- Declare `extendedPlugins = ['composite-engine']` in your format plugin's `build.gradle` so
  the composite engine discovers it at startup.

## Troubleshooting

- **`UnsatisfiedLinkError` / missing native library** — the Parquet/DataFusion plugins need
  their Rust component built and `java.library.path` pointed at it. `./gradlew run` handles
  this automatically; a manually-started node must set `-Djava.library.path` to
  `sandbox/libs/dataformat-native/rust/target/release`.
- **`IllegalArgumentException` changing `index.pluggable.dataformat*`** — these settings are
  final; recreate the index rather than updating them.
- **`_count` returns 0** — expected on a parquet-primary index (see step 4); query via the
  analytics/PPL path instead.
