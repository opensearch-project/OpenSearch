# Liquid Cache

Liquid Cache is an in-memory, decoded-batch Parquet cache for the analytics
(DataFusion) query path. When a query scans Parquet-backed composite indices,
Liquid Cache keeps already-decoded column batches in memory so repeat scans skip
the decode/IO work.

It is an **experimental** feature and is gated behind a feature flag.

## How it works

Liquid Cache is compiled **into** the analytics engine's shared native library
when that library is built with the `liquid_cache` cargo feature (Gradle
`-PliquidCache`). When the feature is off (the default), the engine has no
liquid cache dependency and the integration is an inlined no-op.

When compiled in, the process-global cache is created at node startup and
engages on both scan paths:

- the **listing-table** path, via the cache's DataFusion optimizer added to the
  query session;
- the **indexed** path, via a hook in the per-row-group parquet scan.

Cached decoded batches are served in place of re-decoding Parquet.

```
PPL/SQL query
   -> analytics engine (DataFusion, built with -PliquidCache)
      -> liquid cache (process-global, in-memory decoded batches)
```

This plugin owns the `datafusion.liquid_cache.*` settings, initializes the cache
at startup, and exposes the stats/clear REST surface. It binds the cache's
`lc_*` control symbols directly from the engine library.

## Requirements

- The `analytics-backend-datafusion` plugin must be installed (Liquid Cache
  extends it for classloader access to the shared native library).
- The engine native library must be **built with the `liquid_cache` feature**
  (`-PliquidCache`). Without it, the `lc_*` symbols are absent and the plugin
  loads inert.
- The Liquid Cache experimental feature flag must be enabled.

## Enabling

1. Build the analytics engine native library with the feature enabled
   (`-PliquidCache` / `LIQUID_CACHE=1`), and install the plugin alongside its
   dependency.

2. Enable the feature flag on every node (e.g. in `config/jvm.options` or
   `OPENSEARCH_JAVA_OPTS`):

   ```
   -Dopensearch.experimental.feature.liquid_cache.enabled=true
   ```

3. Start the node. On startup you should see:

   ```
   Liquid Cache bound from the engine native library (liquid_cache feature enabled)
   LiquidCachePlugin: liquid cache initialized and configured
   ```

If the flag is off, or the engine was built without the feature, the plugin
loads but stays inert (a warning is logged) and queries run on the normal engine
path.

## Settings

All settings live under `datafusion.liquid_cache.*`. Dynamic settings can be
changed at runtime via the cluster settings API.

| Setting | Default | Dynamic | Description |
| --- | --- | --- | --- |
| `datafusion.liquid_cache.enabled` | `true` | yes | Turn caching on/off. |
| `datafusion.liquid_cache.size_bytes` | `1073741824` (1 GB) | yes | Cache memory budget in bytes. |
| `datafusion.liquid_cache.eviction_policy` | `lru` | no (set at startup) | Eviction policy: `lru` or `liquid`. |
| `datafusion.liquid_cache.indexed_query.max_columns` | `10` | yes | Max output columns for which the cache engages on the indexed-query path. |
| `datafusion.liquid_cache.listing_table.max_columns` | `4` | yes | Max output columns for which the cache engages on the listing-table path. |

Example:

```
PUT _cluster/settings
{
  "transient": {
    "datafusion.liquid_cache.enabled": true,
    "datafusion.liquid_cache.size_bytes": 2147483648
  }
}
```

## REST endpoints

| Method | Path | Description |
| --- | --- | --- |
| GET | `_plugins/liquid_cache/stats` | Node-local cache counters. |
| POST | `_plugins/liquid_cache/clear` | Clear all in-memory cache entries. |

`stats` returns, per node:

```json
{
  "cache_hit": 0, "cache_miss": 0, "predicate_evals": 0,
  "memory_evictions": 0, "transcodes": 0, "total_entries": 0,
  "memory_usage_bytes": 0, "max_memory_bytes": 1073741824
}
```

A non-zero `max_memory_bytes` indicates the cache was initialized on that node
(the engine was built with the feature and the flag is on).

## Development

Run a local node with the analytics stack and Liquid Cache compiled in
(`-PliquidCache` builds the cache into the engine library):

```
PROTOC=/opt/homebrew/bin/protoc PATH="/opt/homebrew/bin:$PATH" \
./gradlew run -Dsandbox.enabled=true -PrustDebug -PliquidCache \
  -PinstalledPlugins="['arrow-base','arrow-flight-rpc','composite-engine','analytics-engine','parquet-data-format','analytics-backend-datafusion','liquid-cache','analytics-backend-lucene','dsl-query-executor']" \
  -Dtests.jvm.argline="-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true -Dopensearch.experimental.feature.transport.stream.enabled=true -Dopensearch.experimental.feature.liquid_cache.enabled=true"
```

The cache crates live in the standalone Cargo workspace under `src/main/rust/`
(toolchain pinned via `rust-toolchain.toml`) and are compiled into the engine
library as path dependencies when `-PliquidCache` is set. `protoc` must be on
`PATH` for the Rust build.

Run the Rust unit tests for the cache crates:

```
./gradlew :sandbox:plugins:liquid-cache:cargoTest
```
