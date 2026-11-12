# Getting Started with the Pluggable Data Format

> **Experimental.** The pluggable data format and every plugin referenced in this guide
> live under `sandbox/`. They come with no backwards-compatibility or long-term-support
> guarantees and may change or be removed at any time. Do not rely on them in production.

This guide covers running OpenSearch from source with the pluggable data format enabled,
creating a Parquet-backed index, ingesting and verifying data, and running the sandbox test
suites.

## Contents

- [Overview](#overview)
- [Key concepts](#key-concepts)
- [Prerequisites](#prerequisites)
- [Run a node with the storage layer](#run-a-node-with-the-storage-layer)
- [Create a composite-backed index](#create-a-composite-backed-index)
- [Ingest and verify](#ingest-and-verify)
- [Query with PPL](#query-with-ppl)
- [Running the tests](#running-the-tests)
- [Settings reference](#settings-reference)
- [Troubleshooting](#troubleshooting)
- [Further reading](#further-reading)

## Overview

By default, every OpenSearch shard is stored in Lucene. The pluggable data format removes
that assumption: a shard can instead be backed by one or more alternative storage engines,
each contributed by a `DataFormatPlugin` and discovered at node startup through the
`ExtensiblePlugin` SPI (see [`composite-engine/README.md`](README.md)).

The `composite` data format coordinates two roles:

- a **primary** format, the authoritative store that owns merges and commits (defaults to
  `parquet`, a columnar layout suited to analytical scans), and
- zero or more **secondary** formats that receive the same writes, typically `lucene`, so
  full-text and term queries keep working.

Analytical queries against the columnar data go through a separate query stack: a
DataFusion execution backend fronted by a PPL/SQL interface.

## Key concepts

| Term | Meaning |
|---|---|
| Data format | A pluggable storage engine for a shard (`parquet`, `lucene`, ...), contributed by a `DataFormatPlugin`. |
| Composite format | A format that fans writes out to a primary plus zero or more secondaries behind one indexing engine. |
| Primary format | The authoritative format for a composite index; owns merges and commit coordination. |
| Secondary format | An additional format written alongside the primary, e.g. `lucene` for text/term queries. |
| Analytics engine | The query hub that routes PPL/SQL plans to execution backends (DataFusion for Parquet, Lucene for text). |

## Prerequisites

| Requirement | Notes |
|---|---|
| JDK 25 | The data-format plugins (`composite-engine`, `parquet-data-format`, `analytics-backend-datafusion`) build and test against JDK 25, above the repo-wide JDK 21 minimum. Point `JAVA_HOME` at a JDK 25 for these modules, or use `RUNTIME_JAVA_HOME` / `-Druntime.java=25` as described in the [Developer Guide](../../../DEVELOPER_GUIDE.md#custom-runtime-jdk). |
| Rust toolchain | `parquet-data-format` and `analytics-backend-datafusion` include a native Rust component compiled via Cargo during the Gradle build. Install via [rustup](https://rustup.rs/): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh -s -- --default-toolchain stable -y` |
| protoc | Required by the OpenSearch build in general; see the [Developer Guide](../../../DEVELOPER_GUIDE.md#install-prerequisites). |

## Run a node with the storage layer

From the repository root, the `run` task builds the native library and starts a node with
the composite storage stack:

```bash
./gradlew run -PinstalledPlugins='["arrow-base","arrow-flight-rpc","composite-engine","parquet-data-format","analytics-backend-lucene"]'
```

`arrow-base` must be listed first: plugins that extend it fail the install jarHell check
otherwise. Whenever `parquet-data-format` or `analytics-backend-datafusion` is in the
plugin list, the `run` task (`gradle/run.gradle`) automatically enables the
`opensearch.experimental.feature.pluggable.dataformat.enabled` system property and sets
`java.library.path` to the built DataFusion native library, so no manual flags are needed
for this quick start. This node set can create composite indices, ingest, and merge, but
does not serve PPL/SQL queries; see [Query with PPL](#query-with-ppl) for the full stack.

Wait for `[node-1] started` before sending requests.

## Create a composite-backed index

`index.pluggable.dataformat.enabled` and `index.pluggable.dataformat` can only be set at
index creation and cannot be changed afterward:

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
      "message": { "type": "text" },
      "level": { "type": "keyword" },
      "@timestamp": { "type": "date" }
    }
  }
}'
```

`index.composite.secondary_data_formats` accepts a JSON array or a single string.

To default every new index to the composite format instead of specifying it per request,
use the cluster-level settings in the [reference below](#settings-reference).

## Ingest and verify

```bash
curl -X POST "http://localhost:9200/logs-parquet/_doc?refresh=true" -H 'Content-Type: application/json' -d '{
  "message": "connection reset by peer",
  "level": "ERROR",
  "@timestamp": "2026-01-01T00:00:00Z"
}'
```

```bash
curl "http://localhost:9200/_cat/indices/logs-parquet?v"
curl "http://localhost:9200/logs-parquet/_settings?pretty"
```

**`_count` returns 0 on a parquet-primary index, even after a successful ingest.** The
`_count` and `_search` APIs read through the Lucene secondary, which does not materialize
the Parquet-primary rows. This is expected, not data loss. Query the columnar data through
PPL instead.

## Query with PPL

The full analytics stack needs more plugins and JVM configuration than the storage-only
node above, since the analytics engine requires the streaming transport and the DataFusion
native library.

- Plugins: `arrow-base`, `arrow-flight-rpc`, `analytics-engine`, `analytics-backend-datafusion`,
  `composite-engine`, `analytics-backend-lucene`, `dsl-query-executor`, `parquet-data-format`,
  plus the `opensearch-sql` plugin (serves `/_plugins/_ppl`) and its `opensearch-job-scheduler`
  dependency.
- Feature flags: `opensearch.experimental.feature.pluggable.dataformat.enabled=true` and
  `opensearch.experimental.feature.transport.stream.enabled=true` (fragment dispatch is
  streaming-only).
- JVM: Arrow/Netty flags for the DataFusion native library, and
  `-Djava.library.path=<repo>/sandbox/libs/dataformat-native/rust/target/release`.

Exact plugin order, JVM args, and cluster settings drift as the stack evolves, so the
single up-to-date reference is
[`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle),
specifically the `configureAnalyticsCluster` closure. The fastest way to see the whole
stack running end to end is that QA suite (see [Running the tests](#running-the-tests)).

Once configured:

```bash
curl -X POST "http://localhost:9200/_plugins/_ppl" -H 'Content-Type: application/json' -d '{
  "query": "source = logs-parquet | stats count() by level"
}'
```

For component detail, see the
[`analytics-engine`](../analytics-engine/README.md) and
[`analytics-backend-datafusion`](../analytics-backend-datafusion/README.md) READMEs.

## Running the tests

Sandbox plugins use the same test tiers as the rest of OpenSearch. Test tasks are per
project and are not gated by `sandbox.enabled`; that flag only controls whether sandbox
artifacts are bundled into a distribution (`./gradlew assemble -Dsandbox.enabled=true`).

Make sure `JAVA_HOME` points at JDK 25 and Rust/Cargo is installed before running any of
these; the data-format test tasks depend on
`:sandbox:libs:dataformat-native:buildRustLibrary`.

```bash
# Unit tests
./gradlew :sandbox:plugins:composite-engine:test
./gradlew :sandbox:plugins:composite-engine:test --tests "*.CompositeWriterTests"

# Internal cluster tests (in-memory multi-node cluster)
./gradlew :sandbox:plugins:composite-engine:internalClusterTest

# End-to-end REST / QA suite (spins up the full analytics stack)
./gradlew :sandbox:qa:analytics-engine-rest:integTest

# Rust unit tests for the DataFusion native crate, wired into Gradle via cargoTest
./gradlew :sandbox:plugins:analytics-backend-datafusion:cargoTest

# Static checks and formatting
./gradlew :sandbox:plugins:composite-engine:precommit
./gradlew spotlessApply
```

`./gradlew :sandbox:plugins:composite-engine:check` runs unit tests plus static analysis,
but not `internalClusterTest`; invoke that separately, since it is comparatively heavy.

## Settings reference

### Index settings (final, set only at creation)

| Setting | Default | Description |
|---|---|---|
| `index.pluggable.dataformat.enabled` | `false` | Enables the pluggable data format for the index. |
| `index.pluggable.dataformat` | `""` | Data format to use; set to `composite` for primary + secondary. |
| `index.composite.primary_data_format` | `parquet` | Authoritative format that owns merges and commits. |
| `index.composite.secondary_data_formats` | `[]` | Additional formats written alongside the primary. |

### Cluster settings (defaults for new indices)

| Setting | Description |
|---|---|
| `cluster.pluggable.dataformat.enabled` | Cluster-wide default for `index.pluggable.dataformat.enabled`. |
| `cluster.pluggable.dataformat` | Cluster-wide default data format. |
| `cluster.composite.primary_data_format` | Default primary format for new composite indices. |
| `cluster.composite.secondary_data_formats` | Default secondary formats for new composite indices. |

### Feature flags (node JVM system properties)

| Property | Required for |
|---|---|
| `opensearch.experimental.feature.pluggable.dataformat.enabled` | Any composite / pluggable data format index. |
| `opensearch.experimental.feature.transport.stream.enabled` | The PPL/analytics query path. |

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `UnsatisfiedLinkError` or missing native library at startup | The Parquet/DataFusion native library was not built or is not on `java.library.path`. `./gradlew run` and the test tasks build it automatically; a manually started node needs `./gradlew :sandbox:libs:dataformat-native:buildRustLibrary` plus `-Djava.library.path=<repo>/sandbox/libs/dataformat-native/rust/target/release`. |
| `IllegalArgumentException` updating `index.pluggable.dataformat*` | These settings are final; recreate the index instead of updating it. |
| `_count` / `_search` returns 0 documents | Expected on a parquet-primary index; those APIs read the Lucene secondary. Query through PPL. |
| Compile errors referencing FFM or newer APIs | The data-format plugins require JDK 25; check `JAVA_HOME`. |
| PPL query fails or the endpoint is unavailable | The query stack needs the analytics plugins plus `opensearch.experimental.feature.transport.stream.enabled=true`. Compare against `sandbox/qa/analytics-engine-rest/build.gradle`. |

## Further reading

- [`composite-engine/README.md`](README.md), composite engine architecture and key classes.
- [`analytics-engine/README.md`](../analytics-engine/README.md), query hub and SPI wiring.
- [`analytics-backend-datafusion/README.md`](../analytics-backend-datafusion/README.md), native execution backend.
- [`sandbox/qa/analytics-engine-rest/build.gradle`](../../qa/analytics-engine-rest/build.gradle), the authoritative end-to-end cluster configuration.
- [Developer Guide](../../../DEVELOPER_GUIDE.md), building, testing, and the `sandbox` layout.
