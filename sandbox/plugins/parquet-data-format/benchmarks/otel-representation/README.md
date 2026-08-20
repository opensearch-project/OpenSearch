# OTel traces: representation evaluation

Tooling to evaluate how the composite engine's Parquet output behaves when read by **external** query
engines, using an OTel-traces workload. Two things are measured:

1. **Correctness** — do ClickHouse and DataFusion agree on the same file, and does the correlated-group
   invariant hold in the written output? (`compare_engines.py`)
2. **Cost** — how do parallel `LIST` columns compare against the same data flattened into JSON strings?
   (`bench_ch.sh`, `bench_df.sh`)

Nothing here runs in CI. It is reproduction tooling for a one-off evaluation, kept so the numbers can be
re-derived rather than trusted.

## 1. Generate the Parquet file

`OtelTracesParquetGeneratorTests` writes an OTel-traces file through the **real write path** — the same
`ParquetField` implementations the mapping uses, the same `VSRManager`, and the same native arrow-rs
writer — so the artifact is what an index with this mapping actually produces.

```bash
# 12 rows into the test temp dir; this is what runs as a normal test
./gradlew -Dsandbox.enabled=true :sandbox:plugins:parquet-data-format:test \
  --tests "*OtelTracesParquetGeneratorTests*"

# 10M rows to a durable path, for benchmarking
./gradlew -Dsandbox.enabled=true :sandbox:plugins:parquet-data-format:test \
  --tests "*OtelTracesParquetGeneratorTests*" -Dtests.security.manager=false \
  -Dtests.jvm.argline="-Dotel.parquet.out=/tmp/otel_10m.parquet -Dotel.parquet.rows=10000000"
```

| Property | Default | Notes |
| --- | --- | --- |
| `otel.parquet.out` | test temp dir | An external path also needs `-Dtests.security.manager=false` |
| `otel.parquet.rows` | `12` | 10M takes ~45 s and produces ~102 MiB |

Both properties are read by the **forked test JVM**, hence `-Dtests.jvm.argline`; a bare `-D` on the
Gradle command line does not reach it.

The mapping under test, restricted to what is accepted today:

```json
"Events": {
  "type": "nested", "correlated": true,
  "properties": {
    "Name":       { "type": "keyword",     "multi_value": true },
    "Attributes": { "type": "flat_object", "multi_value": true }
  }
}
```

`Events.Timestamp` as `date_nanos` + `multi_value` is **rejected** — only `KeywordParquetField` and
`FlatObjectParquetField` override `supportsMultiValue()`. The second test in the class pins that, so it
starts failing when the remaining type overrides land.

Data shape: 3 spans per trace; every 4th span errored; errored spans carry two events (`exception` then
`retry`), others one (`cache.hit`); every third span carries one link. Attribute cardinality is
deliberately low (3 hosts, 3 SDK languages), which **flatters compression on both sides** — treat any
size figure from this fixture with suspicion.

## 2. Engines

```bash
# ClickHouse (single static binary, reads Parquet directly)
curl -sL -o /tmp/clickhouse https://builds.clickhouse.com/master/macos-aarch64/clickhouse
chmod +x /tmp/clickhouse

# DataFusion
cargo install datafusion-cli
```

Lambda syntax (`array_any_match(arr, e -> ...)`) needs DataFusion **55.0+**, plus
`set datafusion.sql_parser.dialect = databricks`, and that dialect makes `"Events.Name"` a *string
literal* — identifiers need backticks. `UNNEST` works on 54.1 and is the `ARRAY JOIN` equivalent, so it
is the portable choice.

## 3. Cross-engine correctness

```bash
python3 compare_engines.py
```

Runs ~19 query pairs and compares normalised output (`\N`→`NULL`, timestamp separator, trailing `.0`).
Each case declares whether a match or a divergence is expected, so an unexplained difference fails
rather than being absorbed. Notable cases:

- **A vs B** — element-scoped versus document-scoped evaluation of the same predicate. Differs by one
  token (`unnest("Events.Name")` versus `"Events.Name"`) and flips `0` → `3`. Both engines agree on
  both answers, so the false positive is a property of document-granularity evaluation, not of either
  implementation.
- **I** — asserts zero ragged rows in either correlated group, i.e. the parse-time invariant validated
  in the written output rather than in a unit test.
- **F / F2** — on a deliberately ragged file, ClickHouse throws `SIZES_OF_ARRAYS_DONT_MATCH` when both
  arrays are projected but **not** under `count(*)` (the optimizer prunes them, and it then pads to the
  longer array). DataFusion never errors — `arrays_zip` documents "shorter arrays are padded with
  NULLs". Hence write-time enforcement is the only guarantee that does not depend on the query plan.

Harness gotchas, all learned the hard way:

- DataFusion's `--format tsv` emits a header row; ClickHouse's does not. Strip it.
- `set ... dialect` must go in a `-f` script, not `-c`; with `-c` the whole string is parsed before the
  `set` takes effect.
- `array_agg` returns a `List`, which TSV/CSV cannot render — wrap in `array_to_string`.
- Do not detect engine errors by searching output for `"Error"`; the data contains `IOError`.

## 4. Representation benchmark

Build the JSON-string variant of the same data (~3 s, no Rust needed):

```bash
/tmp/clickhouse local -q "
INSERT INTO FUNCTION file('/tmp/otel_10m_json.parquet', Parquet)
SELECT Timestamp, TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName, StatusCode,
       Duration, ResourceAttributes, SpanAttributes,
       toJSONString(arrayMap((n,a) -> map('name',n,'attributes',toJSONString(a)),
                    \`Events.Name\`, \`Events.Attributes\`)) AS events,
       toJSONString(arrayMap((t,s,a) -> map('traceId',t,'spanId',s,'attributes',toJSONString(a)),
                    \`Links.TraceId\`, \`Links.SpanId\`, \`Links.Attributes\`)) AS links
FROM file('/tmp/otel_10m.parquet', Parquet)
SETTINGS engine_file_truncate_on_insert=1, max_threads=8"
```

Stock `datafusion-cli` has no JSON UDFs, so the DataFusion side needs a small binary that registers
`datafusion-functions-json`. `df-json.rs` and `df-json.Cargo.toml` are that binary:

```bash
mkdir -p /tmp/dfjson/src
cp df-json.Cargo.toml /tmp/dfjson/Cargo.toml
cp df-json.rs         /tmp/dfjson/src/main.rs
cargo build --release --manifest-path /tmp/dfjson/Cargo.toml
```

Note the registration call needs the guard dereferenced —
`register_all(&mut *ctx.state_ref().write())` — since `RwLockWriteGuard` itself does not satisfy
`FunctionRegistry`.

```bash
./bench_ch.sh   # ClickHouse, both representations, medians of 5
./bench_df.sh   # DataFusion,  both representations, medians of 5
```

### Results as measured (10M spans, aarch64 macOS)

| Workload | DF: ours | DF: JSON | CH: ours | CH: JSON |
| --- | --- | --- | --- | --- |
| Presence check over all 10M rows | **138 ms** | 294 ms | **272 ms** | 561 ms |
| First link fields, LIMIT 100 | 8 ms | **4 ms** | **218 ms** | 239 ms |
| First event fields, LIMIT 100 | **3 ms** | 4 ms | **172 ms** | 221 ms |
| Explode all events, group by name | **49 ms** | 482 ms | **203 ms** | 1341 ms |
| Element-scoped conjunction | **240 ms** | 598 ms | **400 ms** | 1516 ms |

Parallel `LIST` columns win on four of five workloads, by up to **9.8×**. The mechanism is column
pruning *inside* the group: `unnest("Events.Name")` touches one Parquet leaf and never opens
`Events.Attributes`, whereas `json_get_array(events)` must parse every element — attributes included —
before one key can be read. The gap is widest on explode-and-aggregate and vanishes on `LIMIT 100`
point lookups, where both engines short-circuit and per-column setup dominates.

### On storage — read this before quoting any size

Scoped to the `events`/`links` columns the two representations are at **parity**: 15.65 MB compressed
versus 15.63 MB. A raw file-size comparison (107,101,263 B vs 187,295,500 B) is **not** attributable to
the representation, for two reasons:

1. The files use **different codecs**. Our writer emitted `LZ4_RAW` for `Duration` (62.9 MB) where
   ClickHouse used `ZSTD(1)` (27.8 MB) for identical values — ~35 MB of the gap, on a span-level column
   unrelated to events or links, and 59% of our whole file. Worth investigating independently.
2. The low-cardinality fixture gives the JSON column a **92×** compression ratio (895.9 MB → 9.7 MB)
   that real attribute cardinality would not reproduce.

The real cost of JSON here is not storage but the **4.2× more bytes that must be materialised and
parsed** (306 MB versus 1293 MB uncompressed) — which compresses away on disk and does not in the CPU.

### Caveats

- Only the methodology is shared with the prior JSON-vs-nested comparison that motivated this: that one
  used `List<Struct<...>>` as its "native" side, which cannot prune within the group the way parallel
  columns can. These numbers do not refute it; they refute generalising it.
- Rows 2 and 3 are at the noise floor (3–8 ms).
- The JSON variant nests `attributes` as a JSON string inside each element, costing it a second parse.
  OpenObserve's Serde-flatten layout puts attributes as direct keys, which would be somewhat faster.
- Neither file carries real link/event attributes, so this is not an attributes benchmark.
- **The JSON representation needs no correlation invariant at all** — both fields live inside one
  element object, so a ragged row is unrepresentable. That is a genuine advantage of the blob form. What
  it costs is per-field typing, encoding and statistics, plus the parse cost above.

## 5. Known deviations visible in the output

- Our MAP child group is named `entries`; the Parquet spec requires **`key_value`** (ClickHouse writes
  `key_value`). Both engines read our form, but it is a conformance gap.
- `Timestamp` carries no timezone, i.e. `isAdjustedToUTC=false`.
