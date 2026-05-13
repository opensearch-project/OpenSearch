# Pluggable DataFormat Plugin - Replication & Recovery Test Results

**Date**: 2026-05-06
**Branch**: blockfetchkamal/OpenSearch
**OpenSearch Version**: 3.7.0-SNAPSHOT

## Cluster Configuration

```bash
./gradlew run --preserve-data -PnumNodes=3 -PenableRemoteStore=true \
    -Dsandbox.enabled=true \
    -Dtests.jvm.argline="-Dopensearch.experimental.feature.pluggable.dataformat.enabled=true \
  -Djava.library.path=.../sandbox/libs/dataformat-native/rust/target/release -da" \
    -PinstalledPlugins="['analytics-engine','composite-engine','parquet-data-format','analytics-backend-lucene','analytics-backend-datafusion','test-ppl-frontend']"
```

**Note**: `-da` (disable assertions) is required. With assertions enabled, PPL queries trigger `AssertionError: task N is not in the pending list` in TaskManager which kills nodes.

## Index Configuration

```json
{
  "settings": {
    "index.number_of_shards": 1,
    "index.number_of_replicas": 1,
    "index.refresh_interval": "-1",
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "composite",
    "index.composite.primary_data_format": "parquet",
    "index.replication.type": "SEGMENT",
    "index.composite.secondary_data_formats": []
  },
  "mappings": { "properties": { "name": {"type":"keyword"}, "age": {"type":"integer"} } }
}
```

## Test Results Summary

| # | Test | Result | Notes |
|---|------|--------|-------|
| 1 | Primary-to-Replica Segment Replication | ✅ PASSED | Parquet files replicated identically |
| 2 | Remote Store Upload | ✅ PASSED | Segments + translog uploaded |
| 3 | Shard Relocation | ✅ PASSED | Data integrity maintained |
| 4 | Replica Promotion to Primary | ✅ PASSED | Immediate green, data intact |
| 5 | Recovery from Remote Store | ✅ PASSED | Via _remotestore/_restore API; allocate_stale_primary path still broken |
| 6 | Node Restart Recovery | ✅ PASSED | Local data recovered correctly |
| 7 | Search Validation (PPL) | ⚠️ PARTIAL | Some operations unsupported |

---

## Test 1: Primary-to-Replica Segment Replication ✅ PASSED

### Commands
```bash
# Create index and ingest 10 documents
curl -X PUT 'localhost:9200/test-replication' -H 'Content-Type: application/json' -d '{...}'
curl -X POST 'localhost:9200/test-replication/_bulk' -H 'Content-Type: application/x-ndjson' -d '...'
curl -X POST 'localhost:9200/test-replication/_refresh'
curl 'localhost:9200/_cat/shards/test-replication?v'
```

### Validation
- **Shard allocation**: Primary on runTask-2, Replica on runTask-0
- **Primary parquet files**:
  ```
  runTask-2/.../parquet/_parquet_file_generation_1.parquet  (1,182,515 bytes)
  ```
- **Replica parquet files**:
  ```
  runTask-0/.../parquet/_parquet_file_generation_1.parquet  (1,182,515 bytes)
  ```
- **File sizes match exactly** (1,182,515 bytes on both)
- **Segment files** present on both nodes (segments_3, segments_4 on primary; segments_2, segments_4, segments_5 on replica)
- **PPL query**: `source=test-replication | stats count() as total` → returns 10

### Conclusion
Segment replication correctly copies parquet files from primary to replica with identical content.

---

## Test 2: Remote Store Upload ✅ PASSED

### Commands
```bash
# Check remote store directory
find build/testclusters/remote-store-repo -path "*INDEX_UUID*" -type d
```

### Validation
- **Remote index path mapping**: `remote-store-repo/remote-index-path/remote_path_AQx-ecX1TGGStgROmg0NfQ#2#1#pWp1QuOpRyWEP6JarRWQOg`
- **Segments metadata** (2 files):
  ```
  remote-store-repo/500010011110101/AQx-ecX1TGGStgROmg0NfQ/0/segments/metadata/
    metadata__9223372036854775806__9223372036854775805__...
    metadata__9223372036854775806__9223372036854775806__...
  ```
- **Translog data**:
  ```
  remote-store-repo/Y11101001100000/AQx-ecX1TGGStgROmg0NfQ/0/translog/data/1/
  ```
- **Cluster state**: Stored under `remote-store-repo/cnVuVGFzaw/cluster-state/{CLUSTER_UUID}/`

### Conclusion
Both segment metadata and translog data are uploaded to the remote store repository.

---

## Test 3: Shard Relocation ✅ PASSED

### Commands
```bash
# Move replica from runTask-0 to runTask-1
curl -X POST 'localhost:9200/_cluster/reroute' -H 'Content-Type: application/json' -d '{
  "commands": [{"move": {"index": "test-replication", "shard": 0, "from_node": "runTask-0", "to_node": "runTask-1"}}]
}'
```

### Validation
- **Before**: Primary=runTask-2, Replica=runTask-0
- **During**: State=RELOCATING, recovery_source=PEER
- **After**: Primary=runTask-2, Replica=runTask-1
- **Parquet on new node (runTask-1)**:
  ```
  _parquet_file_generation_1.parquet  (1,182,515 bytes)
  ```
- **Cluster health**: GREEN
- **PPL query**: `stats count() as total` → 10 ✅

### Conclusion
Shard relocation works correctly. Parquet files are transferred to the new node via peer recovery.

---

## Test 4: Replica Promotion to Primary ✅ PASSED

### Commands
```bash
# Cancel primary allocation to force replica promotion
curl -X POST 'localhost:9200/_cluster/reroute' -H 'Content-Type: application/json' -d '{
  "commands": [{"cancel": {"index": "test-replication", "shard": 0, "node": "runTask-2", "allow_primary": true}}]
}'
```

### Validation
- **Before**: Primary=runTask-2, Replica=runTask-1
- **After**: Primary=runTask-1 (promoted), Replica=runTask-2 (new, via peer recovery)
- **Recovery info**:
  - runTask-2 recovered from runTask-1 via PEER (3 files, 2.2mb, 705ms)
  - runTask-1 had prior peer recovery (1 file, 881b, 221ms)
- **Cluster health**: Immediately GREEN
- **Parquet on promoted primary (runTask-1)**: 2 parquet files (generation_1 + generation_2)
- **PPL query**: `stats count() as total` → 10 ✅

### Conclusion
Replica promotion works correctly. The promoted replica becomes primary and a new replica is allocated via peer recovery.

---

## Test 5: Recovery from Remote Store ✅ PASSED (via _remotestore/_restore API)

### Commands
```bash
# Close index
curl -X POST 'localhost:9200/clickbench-6/_close'

# Delete ALL local shard data from all nodes
INDEX_UUID=$(curl -s 'localhost:9200/clickbench-6/_settings' | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['clickbench-6']['settings']['index']['uuid'])")
rm -rf build/testclusters/runTask-0/data/nodes/0/indices/$INDEX_UUID/0/*
rm -rf build/testclusters/runTask-1/data/nodes/0/indices/$INDEX_UUID/0/*
rm -rf build/testclusters/runTask-2/data/nodes/0/indices/$INDEX_UUID/0/*

# Restore from remote store
curl -X POST "localhost:9200/_remotestore/_restore?restore_all_shards=true&wait_for_completion=true&pretty" \
  -H "Content-Type: application/json" \
  -d '{"indices": ["clickbench-6"]}'
```

### Validation
- **Result**: Index recovers successfully, cluster goes GREEN
- **Parquet files restored** on the recovered shard
- **Data intact** after recovery

### Note: allocate_stale_primary path still broken
The `_remotestore/_restore` API works because it uses `RemoteStoreRecoverySource`. However, if you instead just reopen the index (`_open`) without using `_remotestore/_restore`, the auto-allocation uses `EXISTING_STORE` recovery which fails:
```
recoverySourceType=EXISTING_STORE primary=true
IndexShardRecoveryException: shard allocated for local recovery (post api), should exist, but doesn't, current files: []
```
Similarly, `allocate_stale_primary` (reroute API) hardcodes `ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE` and does not check if the index is remote-store-enabled. The fix for this is in `AllocateStalePrimaryAllocationCommand.execute()`.

### Conclusion
Remote store recovery **works** for DataFormatAwareEngine indices via the explicit `_remotestore/_restore` API. The workaround is known and functional. The `allocate_stale_primary` path remains a gap for operators who expect the standard reroute command to work.

---

## Test 6: Node Restart Recovery ✅ PASSED

### Commands
```bash
# Stop cluster (kill gradle worker)
pkill -f "GradleWorkerMain"

# Restart with --preserve-data
./gradlew run --preserve-data -PnumNodes=3 -PenableRemoteStore=true ...
```

### Validation
- **Cluster**: GREEN with 3 nodes within 5 seconds
- **Recovery types**:
  - Primary (runTask-2): `empty_store` recovery (local data intact, 84ms)
  - Replica (runTask-0): `peer` recovery from primary (1 file, 881b, 147ms)
- **Parquet files**: Intact on both nodes (1,182,515 bytes each)
- **PPL query**: `stats count() as total` → 10 ✅

### Conclusion
Full cluster restart with preserved data works correctly. Shards recover from local store and replicas sync via peer recovery.

---

## Test 7: Search Validation (PPL) ⚠️ PARTIAL

### PPL Endpoint
```
POST http://localhost:9200/_analytics/ppl
Content-Type: application/json
{"query": "source=<index> | <commands>"}
```

**Note**: Requires `test-ppl-frontend` plugin to be installed.

### Working Queries

| Query | Result |
|-------|--------|
| `source=test-replication \| stats count() as total` | `{"columns":["total"],"rows":[[10]]}` ✅ |
| `source=test-replication \| fields name, age` | All 10 rows returned ✅ |
| `source=test-replication \| stats count() as c by name` | Correct group-by ✅ |
| `source=test-replication \| where age > 30 \| fields age` | `[35,40,33,45,31]` ✅ |
| `source=test-replication \| stats min(age) as min_age, max(age) as max_age` | `[22, 45]` ✅ |
| `source=test-replication \| stats sum(age) as total_age` | `316` ✅ |
| `source=test-replication \| sort age \| fields age` | Sorted correctly ✅ |
| `source=test-replication \| head 3` | First 3 rows ✅ |

### Failing Queries

| Query | Error |
|-------|-------|
| `where name="alice"` | `No backend supports scalar function [CAST] among [datafusion]` |
| `where age > 30 \| fields name, age` | `can not write type [class org.apache.arrow.vector.util.Text]` |
| `stats avg(age) as avg_age` | `Unable to find binding for call AVG($0)` |
| `sort age \| fields name, age` | `can not write type [class org.apache.arrow.vector.util.Text]` |

### Analysis
- **Integer-only operations work**: count, sum, min, max, sort (integer fields only), where (integer comparisons)
- **Text/keyword field issues**: Returning keyword fields after sort/filter causes Arrow Text serialization errors
- **CAST not supported**: String equality comparisons require CAST which DataFusion backend doesn't support
- **AVG not supported**: The AVG aggregation function has no binding in the DataFusion backend
- **Standard _search API**: Does NOT work — returns `Cannot apply function on indexer class DataFormatAwareEngine directly on IndexShard`

---

## Additional Findings

### 1. Assertions Must Be Disabled (-da)
With JVM assertions enabled (default for `./gradlew run`), PPL queries trigger:
```
java.lang.AssertionError: task N is not in the pending list
    at org.opensearch.tasks.TaskManager$ChannelPendingTaskTracker.removeTask
```
This kills the node and cascades to cluster failure.

### 2. Filesystem Layout
```
build/testclusters/
├── runTask-{0,1,2}/
│   └── data/nodes/0/indices/{INDEX_UUID}/0/
│       ├── index/          (Lucene segment files: segments_N)
│       ├── parquet/        (Parquet data files: _parquet_file_generation_N.parquet)
│       └── translog/       (Transaction log)
└── remote-store-repo/
    ├── remote-index-path/  (Index path mappings)
    ├── {HASH_PREFIX}/{INDEX_UUID}/0/
    │   ├── segments/metadata/  (Segment metadata)
    │   └── translog/data/      (Translog data)
    └── cnVuVGFzaw/cluster-state/  (Cluster state)
```

### 3. Doc Count API Limitation
`_cat/shards` shows blank for `docs` column on DataFormatAwareEngine indices. The `store` column shows size correctly.

---

## Recommendations

1. **Medium**: Fix `allocate_stale_primary` to use `RemoteStoreRecoverySource` for remote-store-enabled indices (AllocateStalePrimaryAllocationCommand.java line ~172)
2. **High**: Fix AssertionError in TaskManager when PPL queries execute (node crash)
3. **Medium**: Add CAST support for keyword field comparisons in DataFusion backend
4. **Medium**: Add AVG aggregation binding in DataFusion backend
5. **Medium**: Fix Arrow Text serialization when returning keyword fields after sort/filter
6. **Low**: Fix doc count reporting in _cat/shards for DataFormatAwareEngine indices
