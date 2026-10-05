# DSL Query-Type REST Tests

Per-query-type integration tests for the parquet/composite data format, run by default against a
**self-provisioned** cluster (`integTest`, wired into `check`), with the option to target an external
OpenSearch server via `restTest -PrestCluster=host:port`. One resource folder per DSL query type; each is
provisioned into a parquet-backed index over REST and its response validated against a committed
expected answer.

This module seeds the suite with a small set of query types. It is intended to grow one folder at a
time as more query types are covered.

## Why REST (and how it runs)

The DSL/analytics/parquet plugin stack targets **JDK 25**. An in-JVM `internalClusterTest` would load
those plugins into the test JVM (pinning it to JDK 25); these tests instead talk HTTP to a cluster via
`OpenSearchRestTestCase`, so the test JVM loads no plugins.

By default the module **self-provisions** that cluster: it applies `opensearch.testclusters` +
`opensearch.standalone-rest-test` + `opensearch.rest-test`, and the `integTest` task boots a
testClusters cluster with the analytics/DataFusion/DSL plugin stack on JDK 25 (see `build.gradle`).
`integTest` is wired into `check`, so it runs in the sandbox CI. A `restTest` task remains for running
the same ITs against an external server you started yourself.

The dataset runner infrastructure (`Dataset` / `DatasetProvisioner` / `DatasetQueryRunner`) is shared:
it comes from the `analytics-qa-fixtures` test-jar (`testArtifacts`), not a local copy. Only the
`Dsl*` classes below are local to this module.

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
"index.composite.secondary_data_formats": ["lucene"]
```

## Java

```
# From the analytics-qa-fixtures test-jar (shared):
Dataset.java              ← dataset descriptor (folder name == index name == query type)
DatasetProvisioner.java   ← reads mapping/bulk, injects parquet settings, creates + ingests
                            (DSL uses provisionAndVerifyParquet: asserts the parquet format took
                            effect and fails on per-item bulk ingest errors)
DatasetQueryRunner.java   ← auto-discovers dsl/q*.json

# Local to this module:
DslQueryTypeCatalog.java  ← one entry per type: type, family, Dataset, tags (known engine gaps)
DslQueryTypesIT.java      ← provisions each type + runs its query, validates against the expected answer
DslResponseValidator.java ← compares a response against the committed expected answer
DslSearchBehaviorIT.java  ← behavioral checks the golden sweep can't express (e.g. randomized-preference stability)
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

## Known engine gaps

Types the engine cannot serve today can be **tagged** on their entry in `DslQueryTypeCatalog` (e.g.
`TAG_MULTI_VALUED_KEYWORD` on `term` / `bool`, whose multi-valued `tags` keyword array parquet rejects).
Tagged parameters are reported as **SKIPPED** with the reason in the test report instead of failing red,
so `check` stays green while the gap stays visible. Remove the tag to reactivate the test once the
engine supports the shape.

## Running

### Default: self-provisioned cluster (`integTest`)

`integTest` boots its own cluster with the full plugin stack and runs the ITs against it — no server to
start. It is wired into `check`, so it also runs in the sandbox CI.

```bash
# Run the suite (self-provisions the cluster)
./gradlew :sandbox:qa:dsl-standalone-server-tests:integTest -Dsandbox.enabled=true

# Just one test
./gradlew :sandbox:qa:dsl-standalone-server-tests:integTest -Dsandbox.enabled=true \
  --tests "org.opensearch.dsl.types.DslQueryTypesIT"
```

### Alternative: external cluster (`restTest`)

Runs the same ITs against a server you started yourself (e.g. `./gradlew run -Dsandbox.enabled=true`
with the same plugin stack). Defaults to `localhost:9200` (cluster name `runTask`).

```bash
# Default: localhost:9200 (cluster runTask)
./gradlew :sandbox:qa:dsl-standalone-server-tests:restTest -Dsandbox.enabled=true

# Custom cluster
./gradlew :sandbox:qa:dsl-standalone-server-tests:restTest -Dsandbox.enabled=true -PrestCluster=host:port
```

## Adding a query type

Create `datasets/<type>/` with `mapping.json` + `bulk.json` + `dsl/q1.json` + `dsl/expected/q1.json`
(the expected answer generated on a vanilla index), and add a matching `e("<type>", family)` entry to
`DslQueryTypeCatalog.all()`. Additional queries per type are auto-discovered as `dsl/q2.json`, ….
