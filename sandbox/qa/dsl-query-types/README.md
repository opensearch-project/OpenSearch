# DSL Query-Type REST Tests

Per-query-type integration tests for the parquet/composite data format, run against a **live, external**
OpenSearch server (your `./gradlew run` sandbox). One resource folder per DSL query type; each is
provisioned into a parquet-backed index over REST and its response validated against a committed
expected answer.

This module seeds the suite with a small set of query types. It is intended to grow one folder at a
time as more query types are covered.

## Why standalone (and why REST)

The DSL/analytics/parquet plugin stack targets **JDK 25**. An in-JVM `internalClusterTest` would load
those plugins into the test JVM (pinning it to JDK 25); these tests instead talk HTTP to an
already-running server via `OpenSearchRestTestCase`, so the test JVM loads no plugins.

The module applies **only** `opensearch.standalone-rest-test` and depends on nothing but
`:test:framework`. The dataset runner infrastructure (`Dataset` / `DatasetProvisioner` /
`DatasetQueryRunner`) is a self-contained local copy in this module.

## Layout — one resource folder per query type

Each query type is a folder under `src/test/resources/datasets/<type>/`:

```
datasets/
  term/
    mapping.json          # index mapping (keeps the literal "number_of_shards" token)
    bulk.json             # sample docs (NDJSON)
    dsl/q1.json           # the DSL query body (auto-discovered as q<N>.json)
    dsl/expected/q1.json  # the expected answer (see below)
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
DatasetQueryRunner.java   ← auto-discovers dsl/q*.json
DslQueryTypeCatalog.java  ← one entry per type: type, family, Dataset
DslQueryTypesIT.java      ← provisions each type + runs its query, validates against the expected answer
DslResponseValidator.java ← compares a response against the committed expected answer
DslTermQueryIT.java       ← focused term-query correctness test (dataset: people)
```

## What is asserted

Each expected answer (`datasets/<type>/dsl/expected/q<N>.json`) is the **true answer** produced on a
vanilla OpenSearch index (default Lucene backend, no parquet settings) and committed. `DslQueryTypesIT`
provisions the dataset with parquet enabled, runs the query, and validates the parquet response against
that expected answer via `DslResponseValidator` (order-independent, numeric tolerance). A type is green
only when the parquet response matches the true answer, so any place parquet deviates surfaces as red.

### Single-valued-tags variants

`term` / `bool` store `tags` as a multi-valued array, which parquet rejects at ingest ("Cannot accept
multiple values for field [tags] of type [keyword]"). The `*_scalar` variants are identical except
`tags` is a single scalar value, which parquet accepts — isolating the multi-value array as the sole
cause and confirming the query type itself is supported once `tags` is single-valued.

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

## Adding a query type

Create `datasets/<type>/` with `mapping.json` + `bulk.json` + `dsl/q1.json` + `dsl/expected/q1.json`
(the expected answer generated on a vanilla index), and add a matching `e("<type>", family)` entry to
`DslQueryTypeCatalog.all()`. Additional queries per type are auto-discovered as `dsl/q2.json`, ….
