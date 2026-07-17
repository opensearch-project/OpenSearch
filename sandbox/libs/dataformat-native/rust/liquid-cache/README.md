# opensearch-liquid-cache (vendored subset)

In-memory-only subset of [liquid-cache](https://github.com/XiangpengHao/liquid-cache),
vendored so the OpenSearch sandbox takes **no Cargo dependency** on the upstream
package.

## Provenance

- Vendored from: https://github.com/cocosz/liquid-cache
- Branch: `lc-opensearch-df54-v2`
- Commit: `8311bccc258756127adbebee3e54140b60fc09ba`
- License: Apache-2.0 (same as upstream)

## What was removed relative to upstream

Everything needed only for the disk tier, client/server mode, or data types the
OpenSearch integration gate never caches:

- **Disk tier**: `t4` store (io-uring), `DiskLiquid`/`DiskArrow` cache entries,
  `SqueezeIoHandler`/`DefaultSqueezeIo`, squeeze-to-disk policies, `ipc.rs`
  serialization and per-array `to_bytes`/`from_bytes`. The cache is memory-only;
  under pressure entries are transcoded (Arrow → Liquid) and then evicted.
- **Client/server**: the whole `liquid-cache-common` crate (arrow-flight/tonic/axum),
  `datafusion-client`, `datafusion-server`.
- **String/binary caching**: `byte_view_array` + FSST (`fsst-rs`). The integration
  gate only engages LC for numeric/date/timestamp/boolean projections.
- **Variant/JSON shredding**: `variant_array`, `parquet-variant-*` deps, variant UDFs.
- **Date32 squeeze**: `squeezed_date32_array` (a squeeze-to-disk optimization).
- **LineageOptimizer**: excluded by the POC already (planning overhead).
- **Tracing**: `fastrace`, `sysinfo`.

## Crates

- `core/` — package `opensearch-liquid-cache-core`, **lib name `liquid_cache`**:
  cache storage (index, budget, eviction policies, transcode) + numeric
  LiquidArray encodings.
- `datafusion/` — package `opensearch-liquid-cache-datafusion`, **lib name
  `liquid_cache_datafusion`**: `LiquidParquetSource` (the ParquetSource
  replacement), reader/stream machinery, `LocalModeOptimizer`.

The lib names intentionally match upstream so vendored code and the
`analytics-backend-datafusion` integration compile with unchanged `use` paths,
which also keeps future re-syncs against upstream reviewable.
