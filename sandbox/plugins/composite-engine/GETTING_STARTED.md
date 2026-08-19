# Getting Started with the Pluggable Data Format

> **Status: Experimental.** The pluggable data format and every plugin referenced in this
> guide live under `sandbox/` and are annotated `@ExperimentalApi`. They provide **no**
> backwards-compatibility or long-term-support guarantees and may change or be removed at
> any time. Do not depend on them in production.

This guide walks you through standing up OpenSearch from source with the pluggable data
format enabled, creating an index backed by columnar (Parquet) storage, ingesting and
querying data, and running the associated test suites.

## Contents

- [Overview](#overview)
- [Architecture at a glance](#architecture-at-a-glance)
- [Key concepts](#key-concepts)
- [Prerequisites](#prerequisites)
- [Quick start: run the storage layer](#quick-start-run-the-storage-layer)
- [Create a composite-backed index](#create-a-composite-backed-index)
- [Ingest and verify](#ingest-and-verify)
- [Query with PPL (full analytics stack)](#query-with-ppl-full-analytics-stack)
- [Running the tests](#running-the-tests)
- [Configuration reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Extending: authoring a new data format](#extending-authoring-a-new-data-format)
- [Further reading](#further-reading)

## Overview

By default, every OpenSearch index is stored in Lucene. The **pluggable data format**
decouples an index from that assumption, allowing a shard to be backed by one or more
alternative storage engines. The engine that coordinates them is the **composite** data
format, which fans each document out to:

- a **primary** format — the authoritative store that owns merges and commit coordination
  (default `parquet`, a columnar format well suited to analytical scans), and
- zero or more **secondary** formats that receive the same writes (commonly `lucene`, so
  full-text and term predicates continue to resolve).

Each storage engine is contributed by a `DataFormatPlugin` and discovered at node startup
through the `ExtensiblePlugin` SPI. Analytical queries over the columnar data are served by
a separate query stack (a DataFusion execution backend fronted by a PPL/SQL interface).

## Architecture at a glance

```
                    ┌──────────────────────────────────────────────┐
   PPL / SQL  ─────▶│  analytics-engine (hub)                       │
   query            │  discovers backends + front ends via SPI      │
                    └───────────────┬──────────────────────────────┘
                                    │ query plan fragments
                     ┌──────────────┴───────────────┐
                     ▼                               ▼
        ┌───────────────────────┐        ┌───────────────────────┐
        │ analytics-backend-    │        │ analytics-backend-     │
        │ datafusion (Parquet)  │        │ lucene (text/terms)    │
        └───────────────────────┘        └───────────────────────┘

   Indexing path:
        document ──▶ composite-engine ──┬──▶ parquet  (primary: merges, commits)
                                        └──▶ lucene   (secondary: text/term index)
```

The composite engine (`sandbox/plugins/composite-engine`) owns the write path; the
analytics engine (`sandbox/plugins/analytics-engine`) and its backends own the read path.
See [`README.md`](README.md) for the composite engine's internal design.

## Key concepts

| Term | Meaning |
|---|---|
| **Data format** | A pluggable storage engine for a shard (e.g. `parquet`, `lucene`), contributed by a `DataFormatPlugin` and identified by a unique `name()`. |
| **Composite format** | A data format that writes each document to a primary plus zero or more secondary formats, presenting them behind a single indexing engine. |
| **Primary format** | The authoritative format for a composite index; owns segment merges and commit coordination. Defaults to `parquet`. |
| **Secondary format** | An additional format that receives the same writes (e.g. `lucene` for text/term queries). |
| **Analytics engine** | The query hub that routes PPL/SQL query plans to execution backends (DataFusion for Parquet, Lucene for text). |

## Prerequisites

| Requirement | Notes |
|---|---|
| **JDK** | The data-format plugins (`composite-engine`, `parquet-data-format`, `analytics-backend-datafusion`) compile and test against **JDK 25**, above the repo-wide JDK 21 minimum. Point `JAVA_HOME` at a JDK 25 when building or testing them. |
| **Rust + Cargo** | The Parquet and DataFusion engines include a native Rust component (`sandbox/libs/dataformat-native`). Install a recent stable toolchain via [rustup](https://rustup.rs/). |
| **protoc** | Required by the OpenSearch build. See the [Developer Guide](../../../DEVELOPER_GUIDE.md#install-prerequisites). |

## Quick start: run the storage layer

From the repository root, launch a single node with the composite storage stack. The Gradle
`run` task automatically enables the pluggable data format feature flag and configures the
native library path whenever `parquet-data-format` or `analytics-backend-datafusion` is in
the plugin list (see `gradle/run.gradle`):

```bash
./gradlew run -PinstalledPlugins='["arrow-base","arrow-flight-rpc","composite-engine","parquet-data-format","analytics-backend-lucene"]'
```

| Plugin | Role |
|---|---|
| `arrow-base` | Apache Arrow runtime. **Must be listed first** — plugins that extend it fail the install jarHell check otherwise. |
| `arrow-flight-rpc` | Arrow Flight transport used by the native path. |
| `composite-engine` | Orchestrates the primary and secondary formats. |
| `parquet-data-format` | The default primary (columnar) format; ships the native Rust component. |
| `analytics-backend-lucene` | Provides the `lucene` secondary format. |

Wait for `[node-1] started` in the console. This node can create composite indices, ingest,
and merge data. To also run analytical **queries**, see
[Query with PPL](#query-with-ppl-full-analytics-stack).

## Create a composite-backed index

The two settings that activate the pluggable data format are **final** — they can only be
set at index-creation time and cannot be updated afterward:

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

> `secondary_data_formats` accepts either a JSON array (`["lucene"]`) or a single string
> (`"lucene"`).

To make every new index use the composite format without specifying it per request, set the
cluster-scoped defaults instead (see the [Configuration reference](#configuration-reference)).

## Ingest and verify

```bash
curl -X POST "http://localhost:9200/logs-parquet/_doc?refresh=true" -H 'Content-Type: application/json' -d '{
  "message": "connection reset by peer",
  "level": "ERROR",
  "@timestamp": "2026-01-01T00:00:00Z"
}'
```

Confirm the shard is healthy and the format is in effect:

```bash
curl "http://localhost:9200/_cat/indices/logs-parquet?v"
curl "http://localhost:9200/logs-parquet/_settings?pretty"
```

> **Expected behavior — `_count` reports 0.** The `_count` and `_search` APIs read through
> the **Lucene secondary** index, which does not materialize the Parquet-primary rows. On a
> parquet-primary index these APIs return **0 documents even after a successful ingest**.
> This is expected, not data loss — query the columnar data through the analytics/PPL path
> described below.

## Query with PPL (full analytics stack)

Analytical queries over Parquet data are served by the analytics engine and its DataFusion
backend, fronted by a PPL interface. This path requires the query plugins **and** the
streaming transport feature flag in addition to the storage layer:

- **Plugins:** `arrow-base`, `arrow-flight-rpc`, `composite-engine`, `parquet-data-format`,
  `analytics-backend-lucene`, `analytics-backend-datafusion`, `analytics-engine`,
  `dsl-query-executor` (and the `opensearch-sql` plugin, which serves `/_plugins/_ppl`).
- **Feature flags:**
  - `opensearch.experimental.feature.pluggable.dataformat.enabled=true`
  - `opensearch.experimental.feature.transport.stream.enabled=true` (fragment dispatch is
    streaming-only)
- **JVM configuration:** the Netty unsafe flags and the DataFusion native library path
  (`-Djava.library.path=<repo>/sandbox/libs/dataformat-native/rust/target/release`).

Once configured, run a query:

```bash
curl -X POST "http://localhost:9200/_plugins/_ppl" -H 'Content-Type: application/json' -d '{
  "query": "source = logs-parquet | stats count() by level"
}'
```

The single authoritative, always-current reference for the exact plugin ordering, JVM
arguments, and cluster settings this stack needs is the QA build definition at
[`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle).
The fastest way to see the complete stack running end to end is to execute that QA suite
(see [Running the tests](#running-the-tests)). For component-level detail, see the
[`analytics-engine`](../analytics-engine/README.md) and
[`analytics-backend-datafusion`](../analytics-backend-datafusion/README.md) READMEs.

## Running the tests

Sandbox plugins use the same test tiers as the rest of OpenSearch. Test tasks run per
project and are **not** gated by the `sandbox.enabled` system property — that flag only
controls whether sandbox artifacts are bundled into a *distribution*
(`./gradlew assemble -Dsandbox.enabled=true`).

> **Before you run:** ensure `JAVA_HOME` points at a **JDK 25** and that **Rust/Cargo** is
> installed — the data-format test tasks depend on
> `:sandbox:libs:dataformat-native:buildRustLibrary`.

### Unit tests

Located in `src/test`; class names end in `Tests`.

```bash
# All unit tests for a plugin
./gradlew :sandbox:plugins:composite-engine:test

# A single class or method
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests"
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests.testRollback"

# Reproduce with a seed, or repeat to surface flakiness
./gradlew :sandbox:plugins:composite-engine:test -Dtests.seed=DEADBEEF
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests.testRollback" -Dtests.iters=50
```

### Internal cluster tests

Located in `src/internalClusterTest`; class names end in `IT`. These run an in-memory
multi-node cluster and are enabled by `apply plugin: 'opensearch.internal-cluster-test'`.

```bash
./gradlew :sandbox:plugins:composite-engine:internalClusterTest
./gradlew :sandbox:plugins:composite-engine:internalClusterTest --tests "*.CompositeParquetIndexIT"
```

Plugins with internal cluster tests today: `composite-engine`, `analytics-engine`,
`analytics-backend-datafusion`, `dsl-query-executor`, `block-cache-foyer`, and the
`native-repository-*` plugins.

### REST / QA integration tests

The end-to-end analytics stack is exercised from the QA modules, which stand up a real test
cluster with the full plugin set and JVM configuration.

```bash
# Full analytics REST suite (default two-node cluster)
./gradlew :sandbox:qa:analytics-engine-rest:integTest

# Purpose-built variants (each configures the cluster differently)
./gradlew :sandbox:qa:analytics-engine-rest:integTestNoMerge     # Parquet segment merge disabled
./gradlew :sandbox:qa:analytics-engine-rest:integTestStreaming   # Arrow Flight streaming
./gradlew :sandbox:qa:analytics-engine-rest:integTestMemtable    # memtable reduce sink

# Coordinator internal cluster tests
./gradlew :sandbox:qa:analytics-engine-coordinator:internalClusterTest

# Run REST tests against an already-running external cluster
./gradlew :sandbox:qa:analytics-engine-rest:restTest -PrestCluster=localhost:9200
```

### Rust tests

The DataFusion native crate has Rust unit and fuzz tests wired into Gradle `check` via a
`cargoTest` task. The fuzz seed follows the build-wide `-Dtests.seed`, so a failing Java
seed reproduces the Rust failure.

```bash
# Through Gradle (runs `cargo test -p opensearch-datafusion --lib`)
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest

# With an explicit seed
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest -PindexedE2eSeed=<hex>

# Or directly in the crate
cd sandbox/libs/dataformat-native/rust && cargo test -p opensearch-datafusion --lib
```

### Verification and formatting

```bash
# Unit tests + static analysis (Spotless, forbidden APIs, ...); for
# analytics-backend-datafusion this also runs cargoTest.
./gradlew :sandbox:plugins:composite-engine:check

# Static / format checks only, and auto-fix formatting
./gradlew :sandbox:plugins:composite-engine:precommit
./gradlew spotlessApply
```

> `check` does **not** include `internalClusterTest` — invoke that task explicitly, as it is
> comparatively heavy.

## Configuration reference

### Index settings (set at creation; final)

| Setting | Default | Description |
|---|---|---|
| `index.pluggable.dataformat.enabled` | `false` | Master switch enabling the pluggable data format for the index. |
| `index.pluggable.dataformat` | `""` | Data format to use; set to `composite` for primary + secondary. |
| `index.composite.primary_data_format` | `parquet` | Authoritative format that owns merges and commits. |
| `index.composite.secondary_data_formats` | `[]` | Additional formats written alongside the primary (array or single string). |

### Cluster settings (dynamic defaults for new indices)

| Setting | Description |
|---|---|
| `cluster.pluggable.dataformat.enabled` | Cluster-wide default for `index.pluggable.dataformat.enabled`. |
| `cluster.pluggable.dataformat` | Cluster-wide default data format (e.g. `composite`). |
| `cluster.composite.primary_data_format` | Default primary format for new composite indices. |
| `cluster.composite.secondary_data_formats` | Default secondary formats for new composite indices. |

### Feature flags (node JVM system properties)

| Property | Required for |
|---|---|
| `opensearch.experimental.feature.pluggable.dataformat.enabled` | Any composite / pluggable data format index. |
| `opensearch.experimental.feature.transport.stream.enabled` | The analytics query path (streaming fragment dispatch). |

## Troubleshooting

| Symptom | Cause and resolution |
|---|---|
| `UnsatisfiedLinkError` or missing native library at startup | The Parquet/DataFusion native library is not built or not on `java.library.path`. `./gradlew run` and the test tasks build it automatically; a manually started node must build it (`./gradlew :sandbox:libs:dataformat-native:buildRustLibrary`) and set `-Djava.library.path=<repo>/sandbox/libs/dataformat-native/rust/target/release`. |
| `IllegalArgumentException` when updating `index.pluggable.dataformat*` | These settings are **final**. Recreate the index rather than updating them. |
| `_count` / `_search` returns 0 documents | Expected on a parquet-primary index — those APIs read the Lucene secondary. Query via the analytics/PPL path. |
| Compilation errors referencing FFM or newer APIs | The data-format plugins require **JDK 25**; verify `JAVA_HOME`. |
| PPL query fails or the analytics endpoint is unavailable | The query stack needs the analytics plugins **and** `opensearch.experimental.feature.transport.stream.enabled=true`. Compare your node against `sandbox/qa/analytics-engine-rest/build.gradle`. |

## Extending: authoring a new data format

To contribute a new storage engine:

1. Implement `DataFormatPlugin`
   (`server/src/main/java/org/opensearch/index/engine/dataformat/DataFormatPlugin.java`):
   - `getDataFormat()` returns your `DataFormat` (unique `name()`, `priority()`, and
     `supportedFields()`),
   - `indexingEngine(IndexingEngineConfig)` returns a per-shard `IndexingExecutionEngine`.
2. Declare `extendedPlugins = ['composite-engine']` in your plugin's `build.gradle` so the
   composite engine discovers your format through the `ExtensiblePlugin` SPI.
3. Register the format name in the index settings above to route data to it.

The SPI contracts (`DataFormat`, `IndexingExecutionEngine`, `Writer`, and the failure model)
are described in detail in [`README.md`](README.md).

## Further reading

- [`composite-engine/README.md`](README.md) — composite engine architecture and key classes.
- [`analytics-engine/README.md`](../analytics-engine/README.md) — query hub and SPI wiring.
- [`analytics-backend-datafusion/README.md`](../analytics-backend-datafusion/README.md) — native execution backend.
- [`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle) — authoritative end-to-end cluster configuration.
- [Developer Guide](../../../DEVELOPER_GUIDE.md) — building, testing, and the `sandbox` layout.
