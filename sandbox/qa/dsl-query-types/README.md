# DSL Query-Type REST Tests

Per-query-type integration tests for the `dsl-query-executor` plugin, run against a **live, external**
OpenSearch server (your `./gradlew run` sandbox). One resource folder per DSL query type demonstrated
in the server's `search/` internalClusterTest folder, executed via a local `DatasetQueryRunner`.

## Why standalone (and why REST)

The DSL/analytics/parquet plugin stack targets **JDK 25**. An in-JVM `internalClusterTest` loads those
plugins into the test JVM (pinning it to JDK 25); these tests instead talk HTTP to an already-running
server via `OpenSearchRestTestCase`, so the test JVM loads no plugins.

The module applies **only** `opensearch.standalone-rest-test` and depends on nothing but
`:test:framework`. It deliberately does **not** depend on `sandbox/qa/analytics-engine-rest` or
`sandbox/plugins/test-ppl-frontend`, so it is unaffected by their build-classpath state. The dataset
runner infrastructure (`Dataset` / `DatasetProvisioner` / `DatasetQueryRunner`) is therefore a **local
copy** in this module rather than a dependency on analytics-engine-rest.

## Layout — one resource folder per query type

Each query type is a folder under `src/test/resources/datasets/<type>/`, mirroring the
`DatasetQueryRunner` convention:

```
datasets/
  term/
    mapping.json          # index mapping (keeps the literal "number_of_shards" token)
    bulk.json             # sample docs (NDJSON)
    dsl/q1.json           # the DSL query body (auto-discovered as q<N>.json)
  match/  range/  bool/  span_near/  geo_shape/  nested/  ...   (57 folders total)
```

`DatasetProvisioner` splices the canonical parquet/composite settings into each `mapping.json` at
provision time — this is the **single place** those settings live:

```
"index.pluggable.dataformat.enabled": true,
"index.pluggable.dataformat": "composite",
"index.composite.primary_data_format": "parquet",
"index.composite.secondary_data_formats": "lucene"
```

## Java

```
Dataset.java              ← dataset descriptor (folder name == index name == query type)
DatasetProvisioner.java   ← reads mapping/bulk, injects parquet settings, creates + ingests
DatasetQueryRunner.java   ← auto-discovers dsl/q*.json and runs each (collects failures, no fail-fast)
DslQueryTypeCatalog.java  ← one entry per type: type, family, Dataset, expected Outcome
DslQueryTypesIT.java      ← provisions each type + runs its queries, asserts observed == expected
```

## What is asserted

Each `DslQueryTypeCatalog` entry has an expected `Outcome`; `DslQueryTypesIT` runs it and asserts
observed == expected:

| Outcome | Meaning | Observed today |
|---|---|---|
| `SUPPORTED` | executes, HTTP 200 (stub result) | `term`, `terms`, `exists`, `match_all`, `no_query` |
| `CONVERSION_ERROR` | supported type + unsupported option/form → `conversion_exception` | `terms`+boost/_name/value_type, `exists`+boost, `terms`-lookup |
| `NOT_PROVISIONABLE` | field mapping rejected at index creation (HTTP 400) | geo (7) + `nested` |
| `UNSUPPORTED` | no translator yet → `runtime_exception` | everything else (39) |

Green when the catalog matches reality; red only on **drift** — a previously-`UNSUPPORTED` type that
starts executing (the signal to promote it to `SUPPORTED`), a `SUPPORTED` regression, or a geo/nested
folder that becomes provisionable. All 57 entries are evaluated before asserting, so one run reports
every drift.

## Running

Start the server with the plugin stack, then:

```bash
# Default: localhost:9200 (cluster runTask)
./gradlew :sandbox:qa:dsl-query-types:restTest -Dsandbox.enabled=true

# Custom cluster
./gradlew :sandbox:qa:dsl-query-types:restTest -Dsandbox.enabled=true -PrestCluster=host:port

# Just this test
./gradlew :sandbox:qa:dsl-query-types:restTest -Dsandbox.enabled=true \
  --tests "org.opensearch.dsl.types.DslQueryTypesIT"
```

## Adding / promoting a query type

- **Add a type:** create `datasets/<type>/` with `mapping.json` + `bulk.json` + `dsl/q1.json`, and add
  a matching `e("<type>", family, outcome)` entry to `DslQueryTypeCatalog.all()`.
- **Add more queries per type:** drop `dsl/q2.json`, `q3.json`, … into the folder — auto-discovered.
- **Promote a type:** when the engine starts supporting a type, the IT logs it under "now work"; flip
  that entry's `Outcome` from `UNSUPPORTED`/`CONVERSION_ERROR` to `SUPPORTED`.
```
