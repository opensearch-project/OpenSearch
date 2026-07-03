# PME End-to-End Integration Tests

These scripts are just meant as temporary fast means to perform actual PME
end-to-end tests. These should be replaced by a more standard solution.

Shell-script end-to-end integration tests for Parquet Modular Encryption (PME)
against a real single-node OpenSearch cluster, using the `mock-pme-kms` plugin
as a dummy KMS (no external service required).

## Prerequisites

| Requirement | Notes |
|---|---|
| JDK 25 | Set `JAVA_HOME`. JDK 21 minimum, 25 recommended for sandbox modules. |
| Rust + cargo | For the native library build step. |
| `curl` | REST test calls. |
| `jq` | JSON assertion parsing. |
| Port 9200 | Must be free on localhost. |

## Files

```
scripts/e2e/
├── setup-cluster.sh              Build plugins and start OpenSearch (background).
├── test-pme.sh                   Run REST tests against a running cluster.
├── test-pme-kms-unavailable.sh   Verify encrypted indices are inaccessible without KMS.
├── teardown.sh                   Stop the cluster started by setup-cluster.sh.
└── run-all.sh                    Full lifecycle: build → start → test → KMS-off → stop.

sandbox/plugins/mock-pme-kms/
└── ...                           Dummy KMS plugin (no real KMS, identity decryption).
```

## Quickstart

```bash
# From the repo root — full lifecycle in one command:
./sandbox/plugins/parquet-data-format/scripts/e2e/run-all.sh

# Skip the Gradle build if plugins are already assembled:
./sandbox/plugins/parquet-data-format/scripts/e2e/run-all.sh --skip-build

# Keep the cluster running after the test run (for manual inspection):
./sandbox/plugins/parquet-data-format/scripts/e2e/run-all.sh --keep-cluster
```

## Step-by-step

```bash
# 1. Build and start the cluster (blocks until cluster is healthy):
./sandbox/plugins/parquet-data-format/scripts/e2e/setup-cluster.sh

# 2. Run the tests against the running cluster:
./sandbox/plugins/parquet-data-format/scripts/e2e/test-pme.sh

# 3. Stop the cluster when done:
./sandbox/plugins/parquet-data-format/scripts/e2e/teardown.sh
```

## Test against an existing cluster

If you already have a cluster running elsewhere:

```bash
./sandbox/plugins/parquet-data-format/scripts/e2e/test-pme.sh --host=http://my-host:9200
```

The test script checks that `parquet-data-format`, `mock-pme-kms`, and
`analytics-backend-lucene` are installed. Use `GET /_cat/plugins` to verify.

The Parquet engine also requires the experimental node feature flag:
```
opensearch.experimental.feature.pluggable.dataformat.enabled: true
```
`setup-cluster.sh` and `run-all.sh` set this automatically for the Gradle run
cluster via `-Dtests.opensearch.opensearch.experimental.feature.pluggable.dataformat.enabled=true`.
The `analytics-backend-lucene` plugin is also required because it provides the
committer used by the pluggable data format engine.

The scripts also pass the Rust native library to the OpenSearch JVM via
`-Dtests.jvm.argline=-Dnative.lib.path=...`. If you use `--skip-build`, make
sure `sandbox/libs/dataformat-native:buildRustLibrary` has already produced the
platform library under `sandbox/libs/dataformat-native/rust/target/release/`.

With the feature flag enabled, successful indexing writes Parquet files under
the shard data path, for example:
```
build/testclusters/runTask-0/data/nodes/0/indices/<uuid>/0/parquet/_parquet_file_generation_*.parquet
```
Normal `_search` requests against these Parquet-backed shards currently fail
with `Cannot apply function on indexer class org.opensearch.index.engine.DataFormatAwareEngine directly on IndexShard`.
The e2e test reports those checks as `XFAIL`. That means the Parquet indexing
path is active, not that Lucene was used.

## What is tested

| Test | Description |
|---|---|
| T1 | Create encrypted Parquet index with `mock-pme` key provider |
| T2 | Index 5 documents + force-refresh |
| T3 | Match-all search returns all 5 documents |
| T4 | Term query on encrypted field returns correct subset + correct source values |
| T5 | Aggregation (SUM + COUNT) over encrypted numeric field returns correct result |
| T6 | Delete encrypted index — subsequent search returns HTTP 404 |
| T7 | Two independent encrypted indices each have separate keyfiles and isolated data |
| T8-A | Unencrypted Parquet index indexes successfully and hits the same `_search` XFAIL |
| T8-B | Plain Lucene index works normally alongside Parquet indices |
| T9 | Create persistent encrypted index readable with KMS; left for Phase 4 |
| T9-A | (Phase 4, no-KMS cluster) Encrypted index returns error or 0 hits |
| T9-B | (Phase 4, no-KMS cluster) Creating new encrypted index is rejected |
| T9-C | (Phase 4, no-KMS cluster) Plain index remains fully readable |

## The mock-pme-kms plugin

`MockPmeKmsPlugin` registers the key provider type `"mock-pme"`. It uses a pure
Java implementation with no Mockito dependency — suitable for installation in a
real node.

Key behaviour:
- `generateDataPair()` — generates a fresh 32-byte key via `SecureRandom`.
  The "encrypted" copy is identical to the raw key (identity wrapping).
- `decryptKey(bytes)` — returns the input unchanged.

This satisfies the PME round-trip contract (`decryptKey(encryptedKey) == rawKey`)
without any external KMS service.

**Not for production use.** Any party that can read the keyfile can decrypt the
data without KMS involvement.

## Index settings used by the tests

```json
{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "parquet",
    "index.store.parquet.crypto.key_provider": "test",
    "index.store.parquet.crypto.key_provider_type": "mock-pme"
  }
}
```
